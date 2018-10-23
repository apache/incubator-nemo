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
package org.apache.nemo.runtime.executor.data;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.nemo.common.Pair;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.executor.bytetransfer.ByteOutputContext;
import org.apache.nemo.runtime.executor.bytetransfer.ByteTransfer;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

/**
 * Executor-side block manager.
 */
@ThreadSafe
public final class PipeManagerWorker {
  private static final Logger LOG = LoggerFactory.getLogger(PipeManagerWorker.class.getName());

  private final String executorId;
  private final SerializerManager serializerManager;

  // To-Executor connections
  private final ByteTransfer byteTransfer;

  // private final SavedConnections savedConnections;

  private final Map<Pair<String, Long>, List<ByteOutputContext>> edgeSrcIndexToContexts;
  private final Map<Pair<String, Long>, CountDownLatch> edgeSrcIndexToLatch;

  @Inject
  private PipeManagerWorker(@Parameter(JobConf.ExecutorId.class) final String executorId,
                            final ByteTransfer byteTransfer,
                            final SerializerManager serializerManager) {
    this.executorId = executorId;
    this.byteTransfer = byteTransfer;
    this.serializerManager = serializerManager;
    this.edgeSrcIndexToContexts = new HashMap<>();
    this.edgeSrcIndexToLatch = new HashMap<>();
  }

  //////////////////////////////////////////////////////////// Main public methods

  public CompletableFuture<DataUtil.IteratorWithNumBytes> read(final int srcTaskIndex,
                                                                   final String runtimeEdgeId,
                                                                   final int dstTaskIndex) {
    /*
    // Get the location of the src task
    final CompletableFuture<ControlMessage.Message> pipeLocationFuture =
      pendingBlockLocationRequest.computeIfAbsent(blockIdWildcard, blockIdToRequest -> {
        // Ask Master for the location.
        // (IMPORTANT): This 'request' effectively blocks the TaskExecutor thread if the block is IN_PROGRESS.
        // We use this property to make the receiver task of a 'push' edge to wait in an Executor for its input data
        // to become available.
        final CompletableFuture<ControlMessage.Message> responseFromMasterFuture = persistentConnectionToMasterMap
          .getMessageSender(MessageEnvironment.BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID).request(
            ControlMessage.Message.newBuilder()
              .setId(RuntimeIdManager.generateMessageId())
              .setListenerId(MessageEnvironment.BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID)
              .setType(ControlMessage.MessageType.RequestBlockLocation)
              .setRequestBlockLocationMsg(
                ControlMessage.RequestBlockLocationMsg.newBuilder()
                  .setExecutorId(executorId)
                  .setBlockIdWildcard(blockIdWildcard)
                  .build())
              .build());
        return responseFromMasterFuture;
      });

    // Descriptor
    final ControlMessage.PipeTransferContextDescriptor descriptor =
      ControlMessage.PipeTransferContextDescriptor.newBuilder()
        .setRuntimeEdgeId(runtimeEdgeId)
        .setDstTaskIndex(dstTaskIndex)
        .build();

    // TODO: Connect to the executor and get iterator.
    return  byteTransfer.newInputContext(targetExecutorId, descriptor.toByteArray())
      .thenApply(context -> new DataUtil.InputStreamIterator(context.getInputStreams(),

        */
    return null;
  }

  public synchronized List<ByteOutputContext> getContexts(final String runtimeEdgeId,
                                                          final long srcTaskIndex,
                                                          final int expectedDstParallelism) {
    final Pair<String, Long> pairKey = Pair.of(runtimeEdgeId, srcTaskIndex);
    final int numAvailableContexts = edgeSrcIndexToContexts.getOrDefault(pairKey, Collections.emptyList()).size();
    if (numAvailableContexts < expectedDstParallelism) {
      // Block
      final CountDownLatch latch = new CountDownLatch(expectedDstParallelism - numAvailableContexts);
      edgeSrcIndexToLatch.put(pairKey, latch);
      try {
        latch.await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      return edgeSrcIndexToContexts.get(pairKey);
    } else if (numAvailableContexts == expectedDstParallelism) {
      return edgeSrcIndexToContexts.get(pairKey);
    } else {
      throw new IllegalStateException(numAvailableContexts + " != " + expectedDstParallelism);
    }
  }

  public synchronized void onOutputContext(final ByteOutputContext outputContext)
    throws InvalidProtocolBufferException {
    final ControlMessage.PipeTransferContextDescriptor descriptor =
      ControlMessage.PipeTransferContextDescriptor.PARSER.parseFrom(outputContext.getContextDescriptor());
    final long srcTaskIndex = descriptor.getSrcTaskIndex();
    final String runtimeEdgeId = descriptor.getRuntimeEdgeId();
    final long dstTaskIndex = descriptor.getDstTaskIndex();

    // Get context list
    final Pair<String, Long> pairKey = Pair.of(runtimeEdgeId, srcTaskIndex);
    edgeSrcIndexToContexts.putIfAbsent(pairKey, new ArrayList<>());
    final List<ByteOutputContext> contexts = edgeSrcIndexToContexts.get(pairKey);

    // Add the context to the list
    contexts.set((int) dstTaskIndex, outputContext);
  }
}
