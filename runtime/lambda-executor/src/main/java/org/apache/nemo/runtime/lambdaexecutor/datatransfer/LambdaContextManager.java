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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import org.apache.nemo.runtime.executor.common.datatransfer.ByteTransferContextSetupMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Manages multiple transport contexts for one channel.
 */
final class LambdaContextManager extends SimpleChannelInboundHandler<ByteTransferContextSetupMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(LambdaContextManager.class);
  private final ChannelGroup channelGroup;
  private final String localExecutorId;
  private final Channel channel;
  private volatile String remoteExecutorId = null;

  private final ConcurrentMap<Integer, ByteOutputContext> outputContextsInitiatedByLocal = new ConcurrentHashMap<>();
  private final AtomicInteger nextOutputTransferIndex = new AtomicInteger(0);

  public LambdaContextManager(final ChannelGroup channelGroup,
                       final String localExecutorId,
                       final Channel channel) {
    LOG.info("New lambda context manager: {} / {}", localExecutorId, channel);
    this.channelGroup = channelGroup;
    this.localExecutorId = localExecutorId;
    this.channel = channel;
  }

  Channel getChannel() {
    return channel;
  }

  @Override
  protected void channelRead0(final ChannelHandlerContext ctx, final ByteTransferContextSetupMessage message)
      throws Exception {
    setRemoteExecutorId(message.getInitiatorExecutorId());
    throw new RuntimeException("message? " + message);
  }

  void onContextExpired(final LambdaByteTransferContext context) {
    // TODO: Send end message!
  }

  <T extends LambdaByteTransferContext> T newContext(final ConcurrentMap<Integer, T> contexts,
                                               final AtomicInteger transferIndexCounter,
                                               final ByteTransferContextSetupMessage.ByteTransferDataDirection dataDirection,
                                               final Function<LambdaByteTransferContext.ContextId, T> contextGenerator,
                                               final String executorId,
                                               final boolean isPipe) {
    setRemoteExecutorId(executorId);
    final int transferIndex = transferIndexCounter.getAndIncrement();
    LOG.info("Output context: srcExecutor: {}, remoteExecutor: {}, transferIndex: {}", transferIndex);
    final LambdaByteTransferContext.ContextId contextId = new LambdaByteTransferContext.ContextId(localExecutorId, executorId, dataDirection, transferIndex, isPipe);
    final T context = contexts.compute(transferIndex, (index, existingContext) -> {
      if (existingContext != null) {
        throw new RuntimeException(String.format("Duplicate ContextId: %s", contextId));
      }
      return contextGenerator.apply(contextId);
    });

    final ByteTransferContextSetupMessage message =
      new ByteTransferContextSetupMessage(localExecutorId,
        context.getContextId().getTransferIndex(),
        context.getContextId().getDataDirection(),
        context.getContextDescriptor(),
        context.getContextId().isPipe());

    channel.writeAndFlush(message).addListener(context.getChannelWriteListener());
    return context;
  }

  ByteOutputContext newOutputContext(final String executorId, final byte[] contextDescriptor, final boolean isPipe) {
    LOG.info("LambdaContextManager: {}", executorId);
    return newContext(outputContextsInitiatedByLocal, nextOutputTransferIndex,
        ByteTransferContextSetupMessage.ByteTransferDataDirection.INITIATOR_SENDS_DATA,
        contextId -> new ByteOutputContext(executorId, contextId, contextDescriptor, this),
        executorId, isPipe);
  }

  private void setRemoteExecutorId(final String executorId) {
    if (remoteExecutorId == null) {
      remoteExecutorId = executorId;
    } else if (!executorId.equals(remoteExecutorId)) {
      throw new RuntimeException(String.format("Wrong ContextManager: (%s != %s)", executorId, remoteExecutorId));
    }
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) {
    channelGroup.add(ctx.channel());
  }

  @Override
  public void channelInactive(final ChannelHandlerContext ctx) {
    channelGroup.remove(ctx.channel());
    final Throwable cause = new Exception("Channel closed");
    throwChannelErrorOnContexts(outputContextsInitiatedByLocal, cause);
  }

  private <T extends LambdaByteTransferContext> void throwChannelErrorOnContexts(final ConcurrentMap<Integer, T> contexts,
                                                                           final Throwable cause) {
    for (final LambdaByteTransferContext context : contexts.values()) {
      context.onChannelError(cause);
    }
  }
}
