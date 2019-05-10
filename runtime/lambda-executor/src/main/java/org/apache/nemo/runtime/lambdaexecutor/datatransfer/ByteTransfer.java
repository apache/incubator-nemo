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

import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import org.apache.nemo.runtime.executor.common.datatransfer.ByteInputContext;
import org.apache.nemo.runtime.executor.common.datatransfer.ByteOutputContext;
import org.apache.nemo.runtime.executor.common.datatransfer.ContextManager;
import org.apache.nemo.runtime.executor.common.datatransfer.VMScalingClientTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 */
@ThreadSafe
public final class ByteTransfer {

  private static final Logger LOG = LoggerFactory.getLogger(ByteTransfer.class);

  private final LambdaByteTransport byteTransport;
  private final ConcurrentMap<String, ChannelFuture> executorIdToChannelFutureMap = new ConcurrentHashMap<>();
  private final ConcurrentMap<Future, ContextManager> channelAndContextManagerMap =
    new ConcurrentHashMap<>();
  private final String localExecutorId;
  private final VMScalingClientTransport clientTransport;

  /**
   * Creates a byte transfer.
   * @param byteTransport provides channels to other executors
   */
  public ByteTransfer(final LambdaByteTransport byteTransport,
                      final String localExecutorId) {
    this.byteTransport = byteTransport;
    this.localExecutorId = localExecutorId;
    this.clientTransport = new VMScalingClientTransport();
  }


  /**
   * Initiate a transfer context to receive data.
   * @param executorId        the id of the remote executor
   * @param contextDescriptor user-provided descriptor for the new context
   * @param isPipe            is pipe
   */
  public CompletableFuture<ByteInputContext> newInputContext(final String executorId,
                                                             final byte[] contextDescriptor,
                                                             final boolean isPipe) {
    LOG.info("New remote input context: {}", executorId);
    return connectTo(executorId).thenApply(manager -> manager.newInputContext(executorId, contextDescriptor, isPipe));
  }

  /**
   * Initiate a transfer context to send data.
   * @param executorId         the id of the remote executor
   * @param contextDescriptor  user-provided descriptor for the new context
   * @param isPipe            is pipe
   * @return a {@link } to which data can be written
   */
  public CompletableFuture<ByteOutputContext> newOutputContext(final String executorId,
                                                               final byte[] contextDescriptor,
                                                               final boolean isPipe) {
    LOG.info("New output context: {}", executorId);
    return connectTo(executorId).thenApply(manager -> manager.newOutputContext(executorId, contextDescriptor, isPipe));
  }

  /**
   * @param remoteExecutorId id of the remote executor
   */
  private CompletableFuture<ContextManager> connectTo(final String remoteExecutorId) {
    final CompletableFuture<ContextManager> completableFuture = new CompletableFuture<>();
    final ChannelFuture channelFuture;
    try {
      channelFuture = executorIdToChannelFutureMap.compute(remoteExecutorId, (executorId, cachedChannelFuture) -> {
        if (cachedChannelFuture != null
            && (cachedChannelFuture.channel().isOpen() || cachedChannelFuture.channel().isActive())) {
          LOG.info("Cached channel of {}/{}", remoteExecutorId, executorId);
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
        /*
        channelAndContextManagerMap.putIfAbsent(future, new LambdaContextManager(
          byteTransport.getChannelGroup(), localExecutorId, channelFuture.channel(), clientTransport));
        LOG.info("Try to create lambda context executor");
        completableFuture.complete(channelAndContextManagerMap.get(future));
        */
      } else {
        executorIdToChannelFutureMap.remove(remoteExecutorId, channelFuture);
        completableFuture.completeExceptionally(future.cause());
      }
    });
    return completableFuture;
  }
}
