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

import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.runtime.common.plan.RuntimeEdge;
import org.apache.nemo.runtime.executor.data.BlockManagerWorker;
import org.apache.nemo.runtime.executor.data.PipeManagerWorker;

/**
 * Represents the output data transfer from a task.
 */
public final class PipeOutputWriter extends OutputWriter {
  private final PipeManagerWorker pipeManagerWorker;
  private final String srcTaskId;
  private final RuntimeEdge runtimeEdge;

  /**
   * Constructor.
   *
   * @param hashRangeMultiplier the {@link org.apache.nemo.conf.JobConf.HashRangeMultiplier}.
   * @param srcTaskId           the id of the source task.
   * @param dstIrVertex         the destination IR vertex.
   * @param runtimeEdge         the {@link RuntimeEdge}.
   * @param blockManagerWorker  the {@link BlockManagerWorker}.
   */
  PipeOutputWriter(final int hashRangeMultiplier,
                   final String srcTaskId,
                   final IRVertex dstIrVertex,
                   final RuntimeEdge<?> runtimeEdge,
                   final PipeManagerWorker pipeManagerWorker) {
    super(hashRangeMultiplier, dstIrVertex, runtimeEdge);
    this.pipeManagerWorker = pipeManagerWorker;
    this.srcTaskId = srcTaskId;
    this.runtimeEdge = runtimeEdge;
  }

  /**
   * Writes output element depending on the communication pattern of the edge.
   * @param element the element to write.
   */
  @Override
  public void write(final Object element) {
    final int index = (Integer) partitioner.partition(element);
    pipeManagerWorker.write(runtimeEdge.getId(), srcTaskId, element, index);
  }

  @Override
  public void close() {
    // Assume this never stops.
  }
}
