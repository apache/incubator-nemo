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
import org.apache.nemo.runtime.executor.common.datatransfer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Manages multiple transport contexts for one channel.
 */
final class LambdaContextManager extends SimpleChannelInboundHandler<ByteTransferContextSetupMessage> implements ContextManager {
  private static final Logger LOG = LoggerFactory.getLogger(LambdaContextManager.class);
  private final ChannelGroup channelGroup;
  private final String localExecutorId;
  private final Channel channel;
  private volatile String remoteExecutorId = null;

  private final ConcurrentMap<Integer, ByteInputContext> inputContextsInitiatedByLocal = new ConcurrentHashMap<>();
  private final ConcurrentMap<Integer, ByteOutputContext> outputContextsInitiatedByLocal = new ConcurrentHashMap<>();
  private final AtomicInteger nextOutputTransferIndex = new AtomicInteger(0);
  private final AtomicInteger nextInputTransferIndex = new AtomicInteger(0);

  private final VMScalingClientTransport vmScalingClientTransport;
  private final AckScheduledService ackScheduledService;

  public LambdaContextManager(final ChannelGroup channelGroup,
                       final String localExecutorId,
                       final Channel channel,
                       final VMScalingClientTransport vmScalingClientTransport,
                       final AckScheduledService ackScheduledService) {
    LOG.info("New lambda context manager: {} / {}", localExecutorId, channel);
    this.channelGroup = channelGroup;
    this.localExecutorId = localExecutorId;
    this.channel = channel;
    this.vmScalingClientTransport = vmScalingClientTransport;
    this.ackScheduledService = ackScheduledService;
  }

  @Override
  public Channel getChannel() {
    return channel;
  }

  @Override
  public ByteInputContext getInputContext(ByteTransferContextSetupMessage.ByteTransferDataDirection dataDirection, int transferIndex) {
    throw new UnsupportedOperationException("unsupported");
  }

  @Override
  public ByteInputContext newInputContext(String executorId, byte[] contextDescriptor, boolean isPipe) {
    return newContext(inputContextsInitiatedByLocal, nextInputTransferIndex,
      ByteTransferContextSetupMessage.ByteTransferDataDirection.INITIATOR_RECEIVES_DATA,
      contextId -> new StreamRemoteByteInputContext(executorId, contextId, contextDescriptor, this,
        ackScheduledService.ackService),
      executorId, isPipe);
  }

  @Override
  protected void channelRead0(final ChannelHandlerContext ctx, final ByteTransferContextSetupMessage message)
      throws Exception {
    // TODO: handle scaleout messages!
    setRemoteExecutorId(message.getInitiatorExecutorId());
    throw new RuntimeException("message? " + message);
  }

  @Override
  public void onContextExpired(final ByteTransferContext context) {
    // TODO: Send end message!
    throw new UnsupportedOperationException("unsupported");
  }

  @Override
  public void onContextCloseLocal(int transferIndex) {
    throw new UnsupportedOperationException("unsupported");
  }

  @Override
  public void onContextRestartLocal(int transferIndex) {
    throw new UnsupportedOperationException("unsupported");
  }

  @Override
  public void onContextStopLocal(int transferIndex) {
    throw new UnsupportedOperationException("unsupported");
  }

  @Override
  public void onContextStop(ByteInputContext context) {
    throw new UnsupportedOperationException("unsupported");
  }

  <T extends ByteTransferContext> T newContext(final ConcurrentMap<Integer, T> contexts,
                                               final AtomicInteger transferIndexCounter,
                                               final ByteTransferContextSetupMessage.ByteTransferDataDirection dataDirection,
                                               final Function<ByteTransferContext.ContextId, T> contextGenerator,
                                               final String executorId,
                                               final boolean isPipe) {
    setRemoteExecutorId(executorId);
    final int transferIndex = transferIndexCounter.getAndIncrement();
    LOG.info("Output context: srcExecutor: {}, remoteExecutor: {}, transferIndex: {}",
      localExecutorId, executorId, transferIndex);
    final ByteTransferContext.ContextId contextId = new ByteTransferContext.ContextId(localExecutorId, executorId, dataDirection, transferIndex, isPipe);
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

  @Override
  public ByteOutputContext newOutputContext(final String executorId, final byte[] contextDescriptor, final boolean isPipe) {
    LOG.info("LambdaContextManager: {}", executorId);
    return newContext(outputContextsInitiatedByLocal, nextOutputTransferIndex,
        ByteTransferContextSetupMessage.ByteTransferDataDirection.INITIATOR_SENDS_DATA,
        contextId -> new RemoteByteOutputContext(executorId, contextId, contextDescriptor, this, vmScalingClientTransport),
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

  private <T extends ByteTransferContext> void throwChannelErrorOnContexts(final ConcurrentMap<Integer, T> contexts,
                                                                           final Throwable cause) {
    for (final ByteTransferContext context : contexts.values()) {
      context.onChannelError(cause);
    }
  }
}
