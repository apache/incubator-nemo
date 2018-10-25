/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nemo.runtime.executor.datatransfer;

import org.apache.nemo.common.DirectByteArrayOutputStream;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.plan.RuntimeEdge;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.runtime.executor.bytetransfer.ByteOutputContext;
import org.apache.nemo.runtime.executor.data.PipeManagerWorker;
import org.apache.nemo.runtime.executor.data.streamchainer.Serializer;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents the output data transfer from a task.
 */
public final class PipeOutputWriter extends OutputWriter {
  private final Serializer serializer;
  private final List<ByteOutputContext> contexts;
  private final List<ByteOutputContext.ByteOutputStream> pipes;

  /**
   * Constructor.
   *
   * @param hashRangeMultiplier the {@link org.apache.nemo.conf.JobConf.HashRangeMultiplier}.
   * @param srcTaskId           the id of the source task.
   * @param dstIrVertex         the destination IR vertex.
   * @param runtimeEdge         the {@link RuntimeEdge}.
   */
  PipeOutputWriter(final int hashRangeMultiplier,
                   final String srcTaskId,
                   final IRVertex dstIrVertex,
                   final RuntimeEdge runtimeEdge,
                   final PipeManagerWorker pipeManagerWorker) {
    super(hashRangeMultiplier, dstIrVertex, runtimeEdge);
    final int dstParallelism = ((StageEdge) runtimeEdge)
      .getDstIRVertex()
      .getPropertyValue(ParallelismProperty.class)
      .orElseThrow(() -> new IllegalStateException());

    System.out.println("VALUES " + ((StageEdge) runtimeEdge).getDstIRVertex().getExecutionProperties().toString());

    // Blocking call
    final List<ByteOutputContext> contexts = pipeManagerWorker
      .initializeOutgoingPipes(runtimeEdge.getId(), RuntimeIdManager.getIndexFromTaskId(srcTaskId), dstParallelism);

    this.contexts = contexts;
    this.pipes = contexts.stream()
      .map(collect -> {
        try {
          return collect.newOutputStream();
        } catch (IOException e) {
          throw new RuntimeException(e); // For now we crash the executor on IOException
        }
      })
      .collect(Collectors.toList());
    this.serializer = pipeManagerWorker.getSerializer(runtimeEdge.getId());
  }

  /**
   * Writes output element
   * @param element the element to write.
   */
  @Override
  public void write(final Object element) {
    try {
      // Get pipe
      final ByteOutputContext.ByteOutputStream pipeToWriteTo;
      if (runtimeEdge.getPropertyValue(CommunicationPatternProperty.class).get()
        .equals(CommunicationPatternProperty.Value.OneToOne)) {
        pipeToWriteTo = pipes.get(0);
      } else {
        pipeToWriteTo = pipes.get((int) partitioner.partition(element));
      }

      // Serialize (Do not compress)
      // TODO: compress
      final DirectByteArrayOutputStream bytesOutputStream = new DirectByteArrayOutputStream();
      final EncoderFactory.Encoder encoder = serializer.getEncoderFactory().create(bytesOutputStream);
      encoder.encode(element);

      // Write
      pipeToWriteTo.write(bytesOutputStream.getBufDirectly());
    } catch (IOException e) {
      throw new RuntimeException(e); // For now we crash the executor on IOException
    }
  }

  @Override
  public void close() {
    contexts.forEach(context -> {
      try {
        context.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }
}
