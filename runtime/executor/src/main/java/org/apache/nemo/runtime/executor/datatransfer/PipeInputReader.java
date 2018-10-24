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
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.runtime.common.plan.RuntimeEdge;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.runtime.executor.data.DataUtil;
import org.apache.nemo.runtime.executor.data.PipeManagerWorker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Represents the input data transfer to a task.
 */
public final class PipeInputReader extends InputReader {
  private final PipeManagerWorker pipeManagerWorker;
  private final String runtimeEdgeId;
  private final int dstParallelism;

  public PipeInputReader(final int dstTaskIdx,
                         final IRVertex srcIRVertex,
                         final RuntimeEdge runtimeEdge,
                         final PipeManagerWorker pipeManagerWorker) {
    super(dstTaskIdx, srcIRVertex, runtimeEdge);
    this.pipeManagerWorker = pipeManagerWorker;
    this.runtimeEdgeId = runtimeEdge.getId();
    this.dstParallelism = ((StageEdge) runtimeEdge).getDstIRVertex().getPropertyValue(ParallelismProperty.class)
      .orElseThrow(() -> new IllegalStateException());
  }

  @Override
  CompletableFuture<DataUtil.IteratorWithNumBytes> readOneToOne() {
    return pipeManagerWorker.read(dstTaskIndex, runtimeEdgeId, dstTaskIndex, dstParallelism);
  }

  @Override
  List<CompletableFuture<DataUtil.IteratorWithNumBytes>> readBroadcast() {
    final int numSrcTasks = this.getSourceParallelism();
    final List<CompletableFuture<DataUtil.IteratorWithNumBytes>> futures = new ArrayList<>();
    for (int srcTaskIdx = 0; srcTaskIdx < numSrcTasks; srcTaskIdx++) {
      futures.add(pipeManagerWorker.read(srcTaskIdx, getId(), dstTaskIndex, dstParallelism));
    }
    return futures;
  }

  @Override
  List<CompletableFuture<DataUtil.IteratorWithNumBytes>> readDataInRange() {
    final int numSrcTasks = this.getSourceParallelism();
    final List<CompletableFuture<DataUtil.IteratorWithNumBytes>> futures = new ArrayList<>();
    for (int srcTaskIdx = 0; srcTaskIdx < numSrcTasks; srcTaskIdx++) {
      futures.add(pipeManagerWorker.read(srcTaskIdx, getId(), dstTaskIndex, dstParallelism));
    }
    return futures;
  }
}
