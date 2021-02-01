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

import org.apache.nemo.common.TaskMetrics;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.runtime.executor.common.ExecutorThreadQueue;
import org.apache.nemo.runtime.executor.common.SerializerManager;
import org.apache.nemo.runtime.executor.common.datatransfer.*;

/**
 */
public final class OffloadingIntermediateDataIOFactory implements IntermediateDataIOFactory {
  private final PipeManagerWorker pipeManagerWorker;
  private final SerializerManager serializerManager;

  public OffloadingIntermediateDataIOFactory(
    final PipeManagerWorker pipeManagerWorker,
    final SerializerManager serializerManager) {
    this.pipeManagerWorker = pipeManagerWorker;
    this.serializerManager = serializerManager;
  }

  @Override
  public OutputWriter createWriter(String srcTaskId, RuntimeEdge<?> runtimeEdge) {
    throw new RuntimeException("exception");
  }

  @Override
  public OutputWriter createPipeWriter(String srcTaskId, RuntimeEdge<?> runtimeEdge, TaskMetrics taskMetrics) {
    return new PipeOutputWriter(srcTaskId, runtimeEdge, pipeManagerWorker,
      serializerManager.getSerializer(runtimeEdge.getId()), taskMetrics);
  }

  @Override
  public InputReader createReader(int dstTaskIdx,
                                  String taskId,
                                  IRVertex srcIRVertex,
                                  RuntimeEdge runtimeEdge,
                                  ExecutorThreadQueue executorThreadQueue) {
    return new PipeInputReader(srcIRVertex, taskId, runtimeEdge,
      serializerManager.getSerializer(runtimeEdge.getId()), executorThreadQueue);
  }
}
