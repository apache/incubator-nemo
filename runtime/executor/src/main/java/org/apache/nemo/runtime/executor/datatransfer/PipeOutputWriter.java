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

import org.apache.nemo.common.TaskMetrics;
import org.apache.nemo.common.exception.UnsupportedCommPatternException;
import org.apache.nemo.common.punctuation.TimestampAndValue;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.runtime.executor.common.WatermarkWithIndex;
import org.apache.nemo.runtime.executor.common.datatransfer.ByteOutputContext;
import org.apache.nemo.runtime.executor.common.datatransfer.PipeManagerWorker;
import org.apache.nemo.runtime.executor.common.Serializer;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.partitioner.Partitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

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
  private final Serializer serializer;
  private final List<String> dstTaskIds;
  private final StageEdge stageEdge;

  private volatile boolean stopped = false;


  private final String stageId;
  private final TaskMetrics taskMetrics;

  /**
   * Constructor.
   *
   * @param srcTaskId           the id of the source task.
   * @param runtimeEdge         the {@link RuntimeEdge}.
   * @param pipeManagerWorker   the pipe manager.
   */
  PipeOutputWriter(final String srcTaskId,
                   final RuntimeEdge runtimeEdge,
                   final PipeManagerWorker pipeManagerWorker,
                   final Serializer serializer,
                   final TaskMetrics taskMetrics) {
    this.stageEdge = (StageEdge) runtimeEdge;
    this.initialized = false;
    this.srcTaskId = srcTaskId;
    this.stageId = RuntimeIdManager.getStageIdFromTaskId(srcTaskId);
    this.taskMetrics = taskMetrics;
    this.pipeManagerWorker = pipeManagerWorker;
    //this.pipeManagerWorker.notifyMaster(runtimeEdge.getId(), RuntimeIdManager.getIndexFromTaskId(srcTaskId));
    this.partitioner = Partitioner
      .getPartitioner(stageEdge.getExecutionProperties(), stageEdge.getDstIRVertex().getExecutionProperties());
    this.runtimeEdge = runtimeEdge;
    this.srcTaskIndex = RuntimeIdManager.getIndexFromTaskId(srcTaskId);
    // this.serializer = serializerManager.getSerializer(runtimeEdge.getId());
    this.serializer = serializer;
    this.dstTaskIds = doInitialize();
  }

  private void writeData(final Object element,
                         final List<String> dstList, final boolean flush) {
    dstList.forEach(dstTask -> {
      pipeManagerWorker.writeData(srcTaskId, dstTask, serializer, element);
    });
  }

  @Override
  public String toString() {
    return runtimeEdge.toString();
  }

  /**
   * Writes output element.
   * This method is not a thread-safe.
   * @param element the element to write.
   */
  @Override
  public void write(final Object element) {

    taskMetrics.incrementOutputElement();
    //executorMetrics.increaseOutputCounter(stageId);

    final TimestampAndValue tis = (TimestampAndValue) element;

    writeData(tis, getPipeToWrite(tis), false);
  }

  @Override
  public void writeWatermark(final Watermark watermark) {
    // LOG.info("Emit watermark of {}: {}",srcTaskId, new Instant(watermark.getTimestamp()));
    pipeManagerWorker.broadcast(srcTaskId,
      dstTaskIds, serializer, new WatermarkWithIndex(watermark, srcTaskIndex));

    // writeData(new WatermarkWithIndex(watermark, srcTaskIndex), pipes, false);

    // 여기서 마스터에게 보내면됨.
    // rendevousServerClient.sendWatermark(srcTaskId, watermark.getTimestamp());
  }

  @Override
  public Optional<Long> getWrittenBytes() {
    return Optional.empty();
  }

  @Override
  public void close() {
    // Send control message and receive ack !!
    // TODO
  }

  @Override
  public Future<Boolean> stop(final String taskId) {
    // send stop message!
    stopped = true;
    throw new RuntimeException("Stop exception " + taskId);

    /*
    final CountDownLatch count = new CountDownLatch(pipes.size());

    for (final ByteOutputContext byteOutputContext : pipes) {
      //LOG.info("Send message {}", pendingMsg);

      byteOutputContext.sendStopMessage((m) -> {
        LOG.info("receive ack from downstream!! {}/{}", runtimeEdge.getId(), runtimeEdge.getSrc());
        count.countDown();
      });
    }

    return new Future<Boolean>() {
      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
      }

      @Override
      public boolean isCancelled() {
        return false;
      }

      @Override
      public boolean isDone() {
        return count.getCount() == 0;
      }

      @Override
      public Boolean get() throws InterruptedException, ExecutionException {
        return count.getCount() == 0;
      }

      @Override
      public Boolean get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return count.getCount() == 0;
      }
    };
    */

    // DO nothing

    /*
    pipes.forEach(pipe -> {
      LOG.info("Pipe stop {}", pipe);
      pipe.stop();
    });
    */
  }

  @Override
  public void restart(final String taskId) {
    /*
    pipes.forEach(pipe -> {
      pipe.restart(taskId);
    });
    */

    stopped = false;
  }

  private List<String> doInitialize() {
    LOG.info("Start - doInitialize() {}", runtimeEdge);
    initialized = true;

    /**********************************************************/

    final Optional<CommunicationPatternProperty.Value> comValue =
      runtimeEdge.getPropertyValue(CommunicationPatternProperty.class);

    final List<String> dstTaskIds;
    if (comValue.get().equals(CommunicationPatternProperty.Value.OneToOne)) {
      dstTaskIds = Collections.singletonList(
        RuntimeIdManager.generateTaskId(stageEdge.getDst().getId(),srcTaskIndex, 0));
      LOG.info("Writing data: edge: {}, Task {}, Dest {}", runtimeEdge.getId(), srcTaskId, srcTaskIndex);
    } else if (comValue.get().equals(CommunicationPatternProperty.Value.BroadCast)
      || comValue.get().equals(CommunicationPatternProperty.Value.Shuffle)) {

      final List<Integer> dstIndices = stageEdge.getDst().getTaskIndices();
      dstTaskIds =
        dstIndices.stream()
          .map(dstTaskIndex ->
            RuntimeIdManager.generateTaskId(stageEdge.getDst().getId(), dstTaskIndex, 0))
          .collect(Collectors.toList());
      LOG.info("Writing data: edge: {}, Task {}, Dest {}", runtimeEdge.getId(), srcTaskId, dstIndices);
    } else {
      throw new UnsupportedCommPatternException(new Exception("Communication pattern not supported"));
    }
    return dstTaskIds;
  }

  private List<String> getPipeToWrite(final TimestampAndValue element) {
    final CommunicationPatternProperty.Value comm =
      (CommunicationPatternProperty.Value) runtimeEdge.getPropertyValue(CommunicationPatternProperty.class).get();
    if (comm.equals(CommunicationPatternProperty.Value.OneToOne)) {
      return Collections.singletonList(dstTaskIds.get(0));
    } else if (comm.equals(CommunicationPatternProperty.Value.BroadCast)) {
      return dstTaskIds;
    } else {
      final int partitionKey = (int) partitioner.partition(element.value);
      //LOG.info("Partition key {} in {} for {}", partitionKey, runtimeEdge.getId(), element);
      return Collections.singletonList(dstTaskIds.get(partitionKey));
    }
  }
}
