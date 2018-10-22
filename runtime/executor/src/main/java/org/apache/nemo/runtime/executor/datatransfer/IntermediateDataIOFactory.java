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

import org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.runtime.common.plan.RuntimeEdge;
import org.apache.nemo.runtime.executor.data.BlockManagerWorker;
import org.apache.nemo.runtime.executor.data.PipeManagerWorker;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Optional;

/**
 * A factory that produces {@link InputReader} and {@link OutputWriter}.
 */
public final class IntermediateDataIOFactory {
  private final PipeManagerWorker pipeManagerWorker;
  private final BlockManagerWorker blockManagerWorker;
  private final int hashRangeMultiplier;

  @Inject
  private IntermediateDataIOFactory(@Parameter(JobConf.HashRangeMultiplier.class) final int hashRangeMultiplier,
                                    final BlockManagerWorker blockManagerWorker,
                                    final PipeManagerWorker pipeManagerWorker) {
    this.hashRangeMultiplier = hashRangeMultiplier;
    this.blockManagerWorker = blockManagerWorker;
    this.pipeManagerWorker = pipeManagerWorker;
  }

  /**
   * Creates an {@link OutputWriter} between two stages.
   *
   * @param srcTaskId   the id of the source task.
   * @param dstIRVertex the {@link IRVertex} that will take the output data as its input.
   * @param runtimeEdge that connects the srcTask to the tasks belonging to dstIRVertex.
   * @return the {@link OutputWriter} created.
   */
  public OutputWriter createWriter(final String srcTaskId,
                                   final IRVertex dstIRVertex,
                                   final RuntimeEdge<?> runtimeEdge) {
    if (isStreaming(runtimeEdge)) {
      return new PipeOutputWriter(hashRangeMultiplier, srcTaskId, dstIRVertex, runtimeEdge, pipeManagerWorker);
    } else {
      return new BlockOutputWriter(hashRangeMultiplier, srcTaskId, dstIRVertex, runtimeEdge, blockManagerWorker);
    }
  }

  /**
   * Creates an {@link InputReader} between two stages.
   *
   * @param dstTaskIdx  the index of the destination task.
   * @param srcIRVertex the {@link IRVertex} that output the data to be read.
   * @param runtimeEdge that connects the tasks belonging to srcIRVertex to dstTask.
   * @return the {@link InputReader} created.
   */
  public InputReader createReader(final int dstTaskIdx,
                                  final IRVertex srcIRVertex,
                                  final RuntimeEdge runtimeEdge) {
    if (isStreaming(runtimeEdge)) {
      return new PipeInputReader(pipeManagerWorker);
    } else {
      return new BlockInputReader(dstTaskIdx, srcIRVertex, runtimeEdge, blockManagerWorker);
    }
  }

  private boolean isStreaming(final RuntimeEdge runtimeEdge) {
    final Optional<DataStoreProperty.Value> dataStoreProperty = runtimeEdge.getPropertyValue(DataStoreProperty.class);
    return dataStoreProperty.isPresent() && dataStoreProperty.get().equals(DataStoreProperty.Value.Streaming);
  }
}
