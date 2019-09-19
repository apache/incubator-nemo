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

import org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.runtime.common.plan.RuntimeEdge;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.runtime.executor.data.BlockManagerWorker;
import org.apache.nemo.runtime.executor.data.PipeManagerWorker;

import javax.inject.Inject;
import java.util.Optional;

/**
 * A factory that produces {@link InputReader} and {@link OutputWriter}.
 */
public final class IntermediateDataIOFactory {
  private final PipeManagerWorker pipeManagerWorker;
  private final BlockManagerWorker blockManagerWorker;

  @Inject
  private IntermediateDataIOFactory(final BlockManagerWorker blockManagerWorker,
                                    final PipeManagerWorker pipeManagerWorker) {
    this.blockManagerWorker = blockManagerWorker;
    this.pipeManagerWorker = pipeManagerWorker;
  }

  /**
   * Creates an {@link OutputWriter} between two stages.
   *
   * @param srcTaskId   the id of the source task.
   * @param runtimeEdge that connects the srcTask to the tasks belonging to dstIRVertex.
   * @return the {@link OutputWriter} created.
   */
  public OutputWriter createWriter(final String srcTaskId,
                                   final RuntimeEdge<?> runtimeEdge) {
    if (isPipe(runtimeEdge)) {
      return new PipeOutputWriter(srcTaskId, runtimeEdge, pipeManagerWorker);
    } else {
      final StageEdge stageEdge = (StageEdge) runtimeEdge;
      return new BlockOutputWriter(srcTaskId, stageEdge.getDstIRVertex(), runtimeEdge, blockManagerWorker);
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
    if (isPipe(runtimeEdge)) {
      return new PipeInputReader(dstTaskIdx, srcIRVertex, runtimeEdge, pipeManagerWorker);
    } else {
      return new BlockInputReader(dstTaskIdx, srcIRVertex, runtimeEdge, blockManagerWorker);
    }
  }

  private boolean isPipe(final RuntimeEdge runtimeEdge) {
    final Optional<DataStoreProperty.Value> dataStoreProperty = runtimeEdge.getPropertyValue(DataStoreProperty.class);
    return dataStoreProperty.isPresent() && dataStoreProperty.get().equals(DataStoreProperty.Value.PIPE);
  }
}
