/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.runtime.executor.datatransfer;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ShuffleExecutorSetProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.TaskIndexToExecutorIDProperty;
import org.apache.nemo.common.partitioner.HashPartitioner;
import org.apache.nemo.common.partitioner.Partitioner;
import org.apache.nemo.common.punctuation.Latencymark;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.plan.RuntimeEdge;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.runtime.executor.transfer.OutputContext;
import org.apache.nemo.runtime.executor.transfer.TransferOutputStream;
import org.apache.nemo.runtime.executor.data.PipeManagerWorker;
import org.apache.nemo.runtime.executor.data.streamchainer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Represents the output data transfer from a task.
 */
public final class PipeOutputWriter implements OutputWriter {
  private static final Logger LOG = LoggerFactory.getLogger(OutputWriter.class.getName());

  private final String srcTaskId;
  private final int srcTaskIndex;
  private final PipeManagerWorker pipeManagerWorker;

  private final Partitioner partitioner;
  private final RuntimeEdge runtimeEdge;

  private boolean initialized;
  private Serializer serializer;
  private List<OutputContext> pipes;

  /**
   * Constructor.
   *
   * @param srcTaskId         the id of the source task.
   * @param runtimeEdge       the {@link RuntimeEdge}.
   * @param pipeManagerWorker the pipe manager.
   */
  PipeOutputWriter(final String srcTaskId,
                   final RuntimeEdge runtimeEdge,
                   final PipeManagerWorker pipeManagerWorker) {
    final StageEdge stageEdge = (StageEdge) runtimeEdge;
    this.initialized = false;
    this.srcTaskId = srcTaskId;
    this.pipeManagerWorker = pipeManagerWorker;
    this.pipeManagerWorker.notifyMaster(runtimeEdge.getId(), RuntimeIdManager.getIndexFromTaskId(srcTaskId));
    this.partitioner = Partitioner
      .getPartitioner(stageEdge.getExecutionProperties(), stageEdge.getDstIRVertex().getExecutionProperties());
    this.runtimeEdge = runtimeEdge;
    this.srcTaskIndex = RuntimeIdManager.getIndexFromTaskId(srcTaskId);
  }

  private void writeData(final Object element, final List<OutputContext> pipeList) {
    pipeList.forEach(pipe -> {
      try (TransferOutputStream pipeToWriteTo = pipe.newOutputStream()) {
        pipeToWriteTo.writeElement(element, serializer);
      } catch (IOException e) {
        throw new RuntimeException(e); // For now we crash the executor on IOException
      }
    });
  }

  /**
   * Writes output element.
   *
   * @param element the element to write.
   */
  @Override
  public void write(final Object element) {
    if (!initialized) {
      doInitialize();
    }

    writeData(element, getPipeToWrite(element));
  }

  @Override
  public void writeWatermark(final Watermark watermark) {
    if (!initialized) {
      doInitialize();
    }

    final WatermarkWithIndex watermarkWithIndex = new WatermarkWithIndex(watermark, srcTaskIndex);
    writeData(watermarkWithIndex, pipes);
  }

  @Override
  public void writeLatencymark(final Latencymark latencymark) {
    if (!initialized) {
      doInitialize();
    }

    writeData(latencymark, pipes);
  }

  @Override
  public Optional<Long> getWrittenBytes() {
    return Optional.empty();
  }

  @Override
  public void close() {
    if (!initialized) {
      // In order to "wire-up" with the receivers waiting for us.:w
      doInitialize();
    }

    pipes.forEach(pipe -> {
      try {
        pipe.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  private void doInitialize() {
    initialized = true;

    // Blocking call
    this.pipes = pipeManagerWorker.getOutputContexts(runtimeEdge, RuntimeIdManager.getIndexFromTaskId(srcTaskId));
    this.serializer = pipeManagerWorker.getSerializer(runtimeEdge.getId());
  }

  private List<OutputContext> getPipeToWrite(final Object element) {
    final Optional<CommunicationPatternProperty.Value> comValueOptional =
      runtimeEdge.getPropertyValue(CommunicationPatternProperty.class);
    final CommunicationPatternProperty.Value comm = comValueOptional.orElseThrow(IllegalStateException::new);

    switch (comm) {
      case ONE_TO_ONE:
        return Collections.singletonList(pipes.get(0));
      case BROADCAST:
        return pipes;
      case PARTIAL_SHUFFLE:
        final List<Pair<String, String>> listOfSrcNodeNames = ((StageEdge) runtimeEdge).getSrc()
          .getPropertyValue(TaskIndexToExecutorIDProperty.class).get().get(srcTaskIndex);
        final String nodeName = listOfSrcNodeNames.get(listOfSrcNodeNames.size() - 1).right();

        final ArrayList<HashSet<String>> setsOfExecutors = ((StageEdge) runtimeEdge).getDst()
          .getPropertyValue(ShuffleExecutorSetProperty.class).get();
        final int numOfSets = setsOfExecutors.size();
        final int dstParallelism = ((StageEdge) runtimeEdge).getDst().getParallelism();
        final List<Integer> listOfDstTaskIdx = IntStream.range(0, dstParallelism)
          .filter(i -> setsOfExecutors.get(i % numOfSets).contains(nodeName))
          .boxed().collect(Collectors.toList());

        final int numOfPartitions = listOfDstTaskIdx.size();
        final int pipeIndex = listOfDstTaskIdx.get(((HashPartitioner) partitioner).partition(element, numOfPartitions));
        return Collections.singletonList(pipes.get(pipeIndex));
      default:
        return Collections.singletonList(pipes.get((int) partitioner.partition(element)));
    }
  }
}
