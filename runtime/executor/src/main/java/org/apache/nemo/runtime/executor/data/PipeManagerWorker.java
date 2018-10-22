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

import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.comm.ControlMessage.ByteTransferContextDescriptor;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.executor.bytetransfer.ByteInputContext;
import org.apache.nemo.runtime.executor.bytetransfer.ByteOutputContext;
import org.apache.nemo.runtime.executor.bytetransfer.ByteTransfer;
import org.apache.nemo.runtime.executor.data.partitioner.Partitioner;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.concurrent.CompletableFuture;

/**
 * Executor-side block manager.
 */
@ThreadSafe
public final class PipeManagerWorker extends  {
  private static final Logger LOG = LoggerFactory.getLogger(PipeManagerWorker.class.getName());

  private final String executorId;
  private final SerializerManager serializerManager;

  // To-Executor connections
  private final ByteTransfer byteTransfer;

  @Inject
  private PipeManagerWorker(@Parameter(JobConf.ExecutorId.class) final String executorId,
                            final ByteTransfer byteTransfer,
                            final SerializerManager serializerManager) {
    this.executorId = executorId;
    this.byteTransfer = byteTransfer;
    this.serializerManager = serializerManager;
  }

  //////////////////////////////////////////////////////////// Main public methods

  public void write(final String runtimeEdgeId,
                    final int producerTaskIndex,
                    final Object element,
                    final int destIndex) {
    // TODO: Get the proper 'Pipe' ByteOutputStream (the one that was saved)
    outputStream = savedOnes.get(destIndex) (list indices)

    // TODO: Writes and flushes (element-wise)
    outputStream.write(element);
  }

  public CompletableFuture<DataUtil.IteratorWithNumBytes> readPipe(final String runtimeEdgeId) {
    // TODO: Get locations
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

    // TODO: Connect to the executor and get iterator.
    return contextFuture
      .thenApply(context -> new DataUtil.InputStreamIterator(context.getInputStreams(),
        serializerManager.getSerializer(runtimeEdgeId)));
  }
}
