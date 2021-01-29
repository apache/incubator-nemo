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
package org.apache.nemo.runtime.lambdaexecutor.datatransfer;

import io.netty.buffer.ByteBuf;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.exception.UnsupportedCommPatternException;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.runtime.executor.common.DataFetcher;
import org.apache.nemo.runtime.executor.common.Serializer;
import org.apache.nemo.runtime.executor.common.TaskExecutor;
import org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage;
import org.apache.nemo.runtime.executor.common.datatransfer.InputReader;
import org.apache.nemo.runtime.executor.common.datatransfer.IteratorWithNumBytes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * Represents the input data transfer to a task.
 */
public final class LambdaPipeInputReader implements InputReader {
  private final PipeManagerWorker pipeManagerWorker;

  private final int dstTaskIndex;

  /**
   * Attributes that specify how we should read the input.
   */
  private final IRVertex srcVertex;
  private final RuntimeEdge runtimeEdge;
  private final String taskId;

  private DataFetcher dataFetcher;
  private final TaskExecutor taskExecutor;

  LambdaPipeInputReader(final int dstTaskIdx,
                        final IRVertex srcIRVertex,
                        final RuntimeEdge runtimeEdge,
                        final PipeManagerWorker pipeManagerWorker,
                        final String taskId,
                        final TaskExecutor taskExecutor) {
    this.dstTaskIndex = dstTaskIdx;
    this.srcVertex = srcIRVertex; this.runtimeEdge = runtimeEdge;
    this.pipeManagerWorker = pipeManagerWorker;
    this.taskId = taskId;
    this.taskExecutor = taskExecutor;
  }


  @Override
  public Future<Integer> stop(final String taksId) {
    return pipeManagerWorker.stop(runtimeEdge, dstTaskIndex, taskId);
  }

  @Override
  public void restart() {
    throw new RuntimeException("Unsupported");
  }

  @Override
  public void setDataFetcher(DataFetcher df) {
    dataFetcher = df;
  }

  @Override
  public List<CompletableFuture<IteratorWithNumBytes>> read() {
    final Optional<CommunicationPatternProperty.Value> comValue =
      runtimeEdge.getPropertyValue(CommunicationPatternProperty.class);

    if (comValue.get().equals(CommunicationPatternProperty.Value.OneToOne)) {
      return Collections.singletonList(pipeManagerWorker.read(dstTaskIndex, runtimeEdge, dstTaskIndex, taskExecutor, dataFetcher));
    } else if (comValue.get().equals(CommunicationPatternProperty.Value.BroadCast)
      || comValue.get().equals(CommunicationPatternProperty.Value.Shuffle)) {
      final int numSrcTasks = InputReader.getSourceParallelism(this);
      final List<CompletableFuture<IteratorWithNumBytes>> futures = new ArrayList<>();
      for (int srcTaskIdx = 0; srcTaskIdx < numSrcTasks; srcTaskIdx++) {
        futures.add(pipeManagerWorker.read(srcTaskIdx, runtimeEdge, dstTaskIndex, taskExecutor, dataFetcher));
      }
      return futures;
    } else {
      throw new UnsupportedCommPatternException(new Exception("Communication pattern not supported"));
    }
  }

  @Override
  public String getTaskId() {
    return null;
  }

  @Override
  public void addControl(TaskControlMessage message) {

  }

  @Override
  public void addData(int pipeIndex, ByteBuf data) {

  }


  @Override
  public IRVertex getSrcIrVertex() {
    return srcVertex;
  }
}
