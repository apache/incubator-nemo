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

import org.apache.nemo.common.exception.UnsupportedCommPatternException;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.runtime.common.plan.RuntimeEdge;
import org.apache.nemo.runtime.executor.data.DataUtil;
import org.apache.nemo.runtime.executor.data.PipeManagerWorker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Represents the input data transfer to a task.
 */
public final class PipeInputReader implements InputReader {
  private final PipeManagerWorker pipeManagerWorker;

  private final int dstTaskIndex;

  /**
   * Attributes that specify how we should read the input.
   */
  private final IRVertex srcVertex;
  private final RuntimeEdge runtimeEdge;

  PipeInputReader(final int dstTaskIdx,
                  final IRVertex srcIRVertex,
                  final RuntimeEdge runtimeEdge,
                  final PipeManagerWorker pipeManagerWorker) {
    this.dstTaskIndex = dstTaskIdx;
    this.srcVertex = srcIRVertex;
    this.runtimeEdge = runtimeEdge;
    this.pipeManagerWorker = pipeManagerWorker;
  }

  @Override
  public List<CompletableFuture<DataUtil.IteratorWithNumBytes>> read() {
    final Optional<CommunicationPatternProperty.Value> comValue =
      runtimeEdge.getPropertyValue(CommunicationPatternProperty.class);

    if (comValue.get().equals(CommunicationPatternProperty.Value.ONE_TO_ONE)) {
      return Collections.singletonList(pipeManagerWorker.read(dstTaskIndex, runtimeEdge, dstTaskIndex));
    } else if (comValue.get().equals(CommunicationPatternProperty.Value.BROADCAST)
      || comValue.get().equals(CommunicationPatternProperty.Value.SHUFFLE)) {
      final int numSrcTasks = InputReader.getSourceParallelism(this);
      final List<CompletableFuture<DataUtil.IteratorWithNumBytes>> futures = new ArrayList<>();
      for (int srcTaskIdx = 0; srcTaskIdx < numSrcTasks; srcTaskIdx++) {
        futures.add(pipeManagerWorker.read(srcTaskIdx, runtimeEdge, dstTaskIndex));
      }
      return futures;
    } else {
      throw new UnsupportedCommPatternException(new Exception("Communication pattern not supported"));
    }
  }

  @Override
  public IRVertex getSrcIrVertex() {
    return srcVertex;
  }
}
