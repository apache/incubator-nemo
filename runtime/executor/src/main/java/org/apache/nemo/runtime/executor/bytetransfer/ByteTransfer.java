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
package org.apache.nemo.runtime.executor.bytetransfer;

import io.netty.channel.*;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.executor.common.datatransfer.*;
import org.apache.nemo.runtime.executor.data.BlockManagerWorker;
import org.apache.nemo.runtime.executor.data.PipeManagerWorker;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Manages channels and exposes interface for {@link BlockManagerWorker}.
 */
@ThreadSafe
public final class ByteTransfer {

  private static final Logger LOG = LoggerFactory.getLogger(ByteTransfer.class);

  private final ByteTransport byteTransport;
  private final ConcurrentMap<String, ChannelFuture> executorIdToChannelFutureMap = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, ContextManager> executorIdLocalContextManagerMap = new ConcurrentHashMap<>();

  private final InjectionFuture<PipeManagerWorker> pipeManagerWorker;
  private final InjectionFuture<BlockManagerWorker> blockManagerWorker;
  private final InjectionFuture<ByteTransfer> byteTransfer;
  private final InjectionFuture<VMScalingClientTransport> vmScalingClientTransport;
  private final InjectionFuture<AckScheduledService> ackScheduledService;
  private final String localExecutorId;

  /**
   * Creates a byte transfer.
   * @param byteTransport provides channels to other executors
   */
  @Inject
  private ByteTransfer(final ByteTransport byteTransport,
                       @Parameter(JobConf.ExecutorId.class) final String localExecutorId,
                       final InjectionFuture<PipeManagerWorker> pipeManagerWorker,
                       final InjectionFuture<BlockManagerWorker> blockManagerWorker,
                       final InjectionFuture<ByteTransfer> byteTransfer,
                       final InjectionFuture<VMScalingClientTransport> vmScalingClientTransport,
                       final InjectionFuture<AckScheduledService> ackScheduledService) {
    this.byteTransport = byteTransport;
    this.localExecutorId = localExecutorId;
    this.pipeManagerWorker = pipeManagerWorker;
    this.blockManagerWorker = blockManagerWorker;
    this.byteTransfer = byteTransfer;
    this.vmScalingClientTransport = vmScalingClientTransport;
    this.ackScheduledService = ackScheduledService;
  }

  /**
   * Initiate a transfer context to receive data.
   * @param executorId        the id of the remote executor
   * @param contextDescriptor user-provided descriptor for the new context
   * @param isPipe            is pipe
   * @return a {@link ByteInputContext} from which the received data can be read
   */
  public CompletableFuture<ByteInputContext> newInputContext(final String executorId,
                                                             final byte[] contextDescriptor,
                                                             final boolean isPipe) {
    return connectTo(executorId).thenApply(manager -> manager.newInputContext(executorId, contextDescriptor, isPipe));
  }

  /**
   * Initiate a transfer context to send data.
   * @param executorId         the id of the remote executor
   * @param contextDescriptor  user-provided descriptor for the new context
   * @param isPipe            is pipe
   * @return a {@link ByteOutputContext} to which data can be written
   */
  public CompletableFuture<ByteOutputContext> newOutputContext(final String executorId,
                                                               final byte[] contextDescriptor,
                                                               final boolean isPipe,
                                                               final boolean isLocal) {
    if (isLocal) {
      LOG.info("New local output context: {}", executorId);
      byteTransport.getAndPutInetAddress(executorId);
      executorIdLocalContextManagerMap.putIfAbsent(executorId,
        new DefaultContextManagerImpl(pipeManagerWorker.get(),
          blockManagerWorker.get(),
          byteTransfer.get(),
          null,
          localExecutorId,
          null,
          vmScalingClientTransport.get(),
          ackScheduledService.get()));
      final ContextManager contextManager = executorIdLocalContextManagerMap.get(executorId);
      return CompletableFuture.completedFuture(
        contextManager.newOutputContext(executorId, contextDescriptor, isPipe));
    } else {
      LOG.info("New remote output context: {}", executorId);
      return connectTo(executorId).thenApply(manager -> manager.newOutputContext(executorId, contextDescriptor, isPipe));
    }
  }

  /**
   * @param remoteExecutorId id of the remote executor
   * @return {@link ContextManager} for the channel to the specified executor
   */
  private CompletableFuture<ContextManager> connectTo(final String remoteExecutorId) {
    final CompletableFuture<ContextManager> completableFuture = new CompletableFuture<>();
    final ChannelFuture channelFuture;
    try {
      channelFuture = executorIdToChannelFutureMap.compute(remoteExecutorId, (executorId, cachedChannelFuture) -> {
        if (cachedChannelFuture != null
            && (cachedChannelFuture.channel().isOpen() || cachedChannelFuture.channel().isActive())) {
          return cachedChannelFuture;
        } else {
          final ChannelFuture future = byteTransport.connectTo(executorId);
          future.channel().closeFuture().addListener(f -> executorIdToChannelFutureMap.remove(executorId, future));
          return future;
        }
      });
    } catch (final RuntimeException e) {
      completableFuture.completeExceptionally(e);
      return completableFuture;
    }
    channelFuture.addListener(future -> {
      if (future.isSuccess()) {
        completableFuture.complete(channelFuture.channel().pipeline().get(ContextManager.class));
      } else {
        executorIdToChannelFutureMap.remove(remoteExecutorId, channelFuture);
        completableFuture.completeExceptionally(future.cause());
      }
    });
    return completableFuture;
  }

  /**
   * Called when a remote executor initiates new transfer context.
   * @param remoteExecutorId  id of the remote executor
   * @param channel           the corresponding {@link Channel}.
   */
  void onNewContextByRemoteExecutor(final String remoteExecutorId, final Channel channel) {
    executorIdToChannelFutureMap.compute(remoteExecutorId, (executorId, cachedChannelFuture) -> {
      if (cachedChannelFuture == null) {
        LOG.debug("Remote {}({}) connected to this executor", new Object[]{executorId, channel.remoteAddress()});
        return channel.newSucceededFuture();
      } else if (channel == cachedChannelFuture.channel()) {
        return cachedChannelFuture;
      } else {
        LOG.warn("Duplicate channel for remote {}({}) and this executor",
            new Object[]{executorId, channel.remoteAddress()});
        return channel.newSucceededFuture();
      }
    });
  }
}
