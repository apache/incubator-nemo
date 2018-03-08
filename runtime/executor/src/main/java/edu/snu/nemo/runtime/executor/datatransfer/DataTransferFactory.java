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
package edu.snu.nemo.runtime.executor.datatransfer;

import edu.snu.nemo.conf.JobConf;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.runtime.common.plan.RuntimeEdge;
import edu.snu.nemo.runtime.common.plan.physical.Task;
import edu.snu.nemo.runtime.executor.data.BlockManagerWorker;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * A factory that produces {@link InputReader} and {@link OutputWriter}.
 */
public final class DataTransferFactory {

  private final BlockManagerWorker blockManagerWorker;
  private final int hashRangeMultiplier;

  @Inject
  public DataTransferFactory(@Parameter(JobConf.HashRangeMultiplier.class) final int hashRangeMultiplier,
                             final BlockManagerWorker blockManagerWorker) {
    this.hashRangeMultiplier = hashRangeMultiplier;
    this.blockManagerWorker = blockManagerWorker;
  }

  /**
   * Creates an {@link OutputWriter} between two stages.
   *
   * @param srcTask     the {@link Task} that outputs the data to be written.
   * @param srcTaskIdx  the index of the source task.
   * @param dstIRVertex the {@link IRVertex} that will take the output data as its input.
   * @param runtimeEdge that connects the srcTask to the tasks belonging to dstIRVertex.
   * @return the {@link OutputWriter} created.
   */
  public OutputWriter createWriter(final Task srcTask,
                                   final int srcTaskIdx,
                                   final IRVertex dstIRVertex,
                                   final RuntimeEdge<?> runtimeEdge) {
    return new OutputWriter(hashRangeMultiplier, srcTaskIdx,
        srcTask.getIrVertexId(), dstIRVertex, runtimeEdge, blockManagerWorker);
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
    return new InputReader(dstTaskIdx, srcIRVertex, runtimeEdge, blockManagerWorker);
  }
}
