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

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;

import org.apache.nemo.runtime.executor.common.Serializer;
import org.apache.nemo.runtime.executor.common.datatransfer.PipeTransferContextDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Two threads use this class
 * - Network thread: Saves pipe connections created from destination tasks.
 * - Task executor thread: Creates new pipe connections to destination tasks (read),
 *                         or retrieves a saved pipe connection (write)
 */
@ThreadSafe
public final class PipeManagerWorker {
  private static final Logger LOG = LoggerFactory.getLogger(PipeManagerWorker.class.getName());

  private final String executorId;
  private final Map<Integer, String> dstTaskIndexTargetExecutorMap;

  // To-Executor connections
  private final ByteTransfer byteTransfer;

  public PipeManagerWorker(final String executorId,
                           final ByteTransfer byteTransfer,
                           final Map<Integer, String> dstTaskIndexTargetExecutorMap) {
    this.executorId = executorId;
    this.byteTransfer = byteTransfer;
    this.dstTaskIndexTargetExecutorMap = dstTaskIndexTargetExecutorMap;
  }


  public CompletableFuture<ByteOutputContext> write(final int srcTaskIndex,
                    final RuntimeEdge runtimeEdge,
                    final int dstTaskIndex) {
    final String runtimeEdgeId = runtimeEdge.getId();
    final String targetExecutorId = dstTaskIndexTargetExecutorMap.get(dstTaskIndex);

    // Descriptor
    final PipeTransferContextDescriptor descriptor =
      new PipeTransferContextDescriptor(
        runtimeEdgeId,
        srcTaskIndex,
        dstTaskIndex,
        getNumOfInputPipeToWait(runtimeEdge));

    LOG.info("Writer descriptor: runtimeEdgeId: {}, srcTaskIndex: {}, dstTaskIndex: {}, getNumOfInputPipe:{} ",
      runtimeEdgeId, srcTaskIndex, dstTaskIndex, getNumOfInputPipeToWait(runtimeEdge));
    // Connect to the executor
    return byteTransfer.newOutputContext(targetExecutorId, descriptor.encode(), true)
      .thenApply(context -> context);
  }

  private int getNumOfInputPipeToWait(final RuntimeEdge runtimeEdge) {
    final int srcParallelism = ((StageEdge) runtimeEdge).getSrc().getPropertyValue(ParallelismProperty.class)
      .orElseThrow(() -> new IllegalStateException());
    final CommunicationPatternProperty.Value commPattern = ((StageEdge) runtimeEdge)
      .getPropertyValue(CommunicationPatternProperty.class)
      .orElseThrow(() -> new IllegalStateException());
    return commPattern.equals(CommunicationPatternProperty.Value.OneToOne) ? 1 : srcParallelism;
  }
}
