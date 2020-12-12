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
package org.apache.nemo.runtime.executor.transfer;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import org.apache.nemo.runtime.common.comm.ControlMessage.ByteTransferContextSetupMessage;
import org.apache.nemo.runtime.common.comm.ControlMessage.ByteTransferDataDirection;
import org.apache.nemo.runtime.executor.transfer.ByteTransferContext.ContextId;
import org.apache.nemo.runtime.executor.data.BlockManagerWorker;
import org.apache.nemo.runtime.executor.data.PipeManagerWorker;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Manages multiple transport contexts for one channel.
 */
final class ContextManager extends SimpleChannelInboundHandler<ByteTransferContextSetupMessage> {

  private final PipeManagerWorker pipeManagerWorker;
  private final BlockManagerWorker blockManagerWorker;
  private final ByteTransfer byteTransfer;
  private final ChannelGroup channelGroup;
  private final String localExecutorId;
  private final Channel channel;
  private volatile String remoteExecutorId = null;

  private final ConcurrentMap<Integer, ByteInputContext> inputContextsInitiatedByLocal = new ConcurrentHashMap<>();
  private final ConcurrentMap<Integer, ByteOutputContext> outputContextsInitiatedByLocal = new ConcurrentHashMap<>();
  private final ConcurrentMap<Integer, ByteInputContext> inputContextsInitiatedByRemote = new ConcurrentHashMap<>();
  private final ConcurrentMap<Integer, ByteOutputContext> outputContextsInitiatedByRemote = new ConcurrentHashMap<>();
  private final AtomicInteger nextInputTransferIndex = new AtomicInteger(0);
  private final AtomicInteger nextOutputTransferIndex = new AtomicInteger(0);

  /**
   * Creates context manager for this channel.
   *
   * @param pipeManagerWorker  provides handler for new contexts by remote executors
   * @param blockManagerWorker provides handler for new contexts by remote executors
   * @param byteTransfer       provides channel caching
   * @param channelGroup       to cleanup this channel when closing {@link ByteTransport}
   * @param localExecutorId    local executor id
   * @param channel            the {@link Channel} to manage
   */
  ContextManager(final PipeManagerWorker pipeManagerWorker,
                 final BlockManagerWorker blockManagerWorker,
                 final ByteTransfer byteTransfer,
                 final ChannelGroup channelGroup,
                 final String localExecutorId,
                 final Channel channel) {
    this.pipeManagerWorker = pipeManagerWorker;
    this.blockManagerWorker = blockManagerWorker;
    this.byteTransfer = byteTransfer;
    this.channelGroup = channelGroup;
    this.localExecutorId = localExecutorId;
    this.channel = channel;
  }

  /**
   * @return channel for this context manager.
   */
  Channel getChannel() {
    return channel;
  }

  /**
   * Returns {@link ByteInputContext} to provide {@link io.netty.buffer.ByteBuf}s on.
   *
   * @param dataDirection the data direction
   * @param transferIndex transfer index
   * @return the {@link ByteInputContext} corresponding to the pair of {@code dataDirection} and {@code transferIndex}
   */
  ByteInputContext getInputContext(final ByteTransferDataDirection dataDirection,
                                   final int transferIndex) {
    final ConcurrentMap<Integer, ByteInputContext> contexts =
      dataDirection == ByteTransferDataDirection.INITIATOR_SENDS_DATA
        ? inputContextsInitiatedByRemote : inputContextsInitiatedByLocal;
    return contexts.get(transferIndex);
  }

  /**
   * Responds to new transfer contexts by a remote executor.
   *
   * @param ctx     netty {@link ChannelHandlerContext}
   * @param message context setup message from the remote executor
   * @throws Exception exceptions from handler
   */
  @Override
  protected void channelRead0(final ChannelHandlerContext ctx, final ByteTransferContextSetupMessage message)
    throws Exception {
    setRemoteExecutorId(message.getInitiatorExecutorId());
    byteTransfer.onNewContextByRemoteExecutor(message.getInitiatorExecutorId(), channel);
    final ByteTransferDataDirection dataDirection = message.getDataDirection();
    final int transferIndex = message.getTransferIndex();
    final boolean isPipe = message.getIsPipe();
    final ContextId contextId =
      new ContextId(remoteExecutorId, localExecutorId, dataDirection, transferIndex, isPipe);
    final byte[] contextDescriptor = message.getContextDescriptor().toByteArray();

    if (dataDirection == ByteTransferDataDirection.INITIATOR_SENDS_DATA) {
      final ByteInputContext context = inputContextsInitiatedByRemote.compute(transferIndex, (index, existing) -> {
        if (existing != null) {
          throw new RuntimeException(String.format("Duplicate ContextId: %s", contextId));
        }
        return new ByteInputContext(remoteExecutorId, contextId, contextDescriptor, this);
      });

      if (isPipe) {
        pipeManagerWorker.onInputContext(context);
      } else {
        blockManagerWorker.onInputContext(context);
      }
    } else {
      final ByteOutputContext context = outputContextsInitiatedByRemote.compute(transferIndex, (idx, existing) -> {
        if (existing != null) {
          throw new RuntimeException(String.format("Duplicate ContextId: %s", contextId));
        }
        return new ByteOutputContext(remoteExecutorId, contextId, contextDescriptor, this);
      });
      if (isPipe) {
        pipeManagerWorker.onOutputContext(context);
      } else {
        blockManagerWorker.onOutputContext(context);
      }
    }
  }

  /**
   * Removes the specified contexts from map.
   *
   * @param context the {@link ByteTransferContext} to remove.
   */
  void onContextExpired(final ByteTransferContext context) {
    final ContextId contextId = context.getContextId();
    final ConcurrentMap<Integer, ? extends ByteTransferContext> contexts = context instanceof ByteInputContext
      ? (contextId.getDataDirection() == ByteTransferDataDirection.INITIATOR_SENDS_DATA
      ? inputContextsInitiatedByRemote : inputContextsInitiatedByLocal)
      : (contextId.getDataDirection() == ByteTransferDataDirection.INITIATOR_SENDS_DATA
      ? outputContextsInitiatedByLocal : outputContextsInitiatedByRemote);
    contexts.remove(contextId.getTransferIndex(), context);
  }

  /**
   * Initiates a context and stores to the specified map.
   *
   * @param contexts             map for storing context
   * @param transferIndexCounter counter for generating transfer index
   * @param dataDirection        data direction to include in the context id
   * @param contextGenerator     a function that returns context from context id
   * @param executorId           id of the remote executor
   * @param <T>                  {@link ByteInputContext} or {@link ByteOutputContext}
   * @param isPipe               is a pipe context
   * @return generated context
   */
  <T extends ByteTransferContext> T newContext(final ConcurrentMap<Integer, T> contexts,
                                               final AtomicInteger transferIndexCounter,
                                               final ByteTransferDataDirection dataDirection,
                                               final Function<ContextId, T> contextGenerator,
                                               final String executorId,
                                               final boolean isPipe) {
    setRemoteExecutorId(executorId);
    final int transferIndex = transferIndexCounter.getAndIncrement();
    final ContextId contextId = new ContextId(localExecutorId, executorId, dataDirection, transferIndex, isPipe);
    final T context = contexts.compute(transferIndex, (index, existingContext) -> {
      if (existingContext != null) {
        throw new RuntimeException(String.format("Duplicate ContextId: %s", contextId));
      }
      return contextGenerator.apply(contextId);
    });
    channel.writeAndFlush(context).addListener(context.getChannelWriteListener());
    return context;
  }

  /**
   * Create a new {@link ByteInputContext}.
   *
   * @param executorId        target executor id
   * @param contextDescriptor the context descriptor
   * @param isPipe            is pipe
   * @return new {@link ByteInputContext}
   */
  ByteInputContext newInputContext(final String executorId, final byte[] contextDescriptor, final boolean isPipe) {
    return newContext(inputContextsInitiatedByLocal, nextInputTransferIndex,
      ByteTransferDataDirection.INITIATOR_RECEIVES_DATA,
      contextId -> new ByteInputContext(executorId, contextId, contextDescriptor, this),
      executorId, isPipe);
  }

  /**
   * Create a new {@link ByteOutputContext}.
   *
   * @param executorId        target executor id
   * @param contextDescriptor the context descriptor
   * @param isPipe            is pipe
   * @return new {@link ByteOutputContext}
   */
  ByteOutputContext newOutputContext(final String executorId, final byte[] contextDescriptor, final boolean isPipe) {
    return newContext(outputContextsInitiatedByLocal, nextOutputTransferIndex,
      ByteTransferDataDirection.INITIATOR_SENDS_DATA,
      contextId -> new ByteOutputContext(executorId, contextId, contextDescriptor, this),
      executorId, isPipe);
  }

  /**
   * Set this contest manager as connected to the specified remote executor.
   *
   * @param executorId the remote executor id
   */
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
    throwChannelErrorOnContexts(inputContextsInitiatedByLocal, cause);
    throwChannelErrorOnContexts(outputContextsInitiatedByLocal, cause);
    throwChannelErrorOnContexts(inputContextsInitiatedByRemote, cause);
    throwChannelErrorOnContexts(outputContextsInitiatedByRemote, cause);
  }

  /**
   * Invoke {@link ByteTransferContext#onChannelError(Throwable)} on the specified contexts.
   *
   * @param contexts map storing the contexts
   * @param cause    the error
   * @param <T>      {@link ByteInputContext} or {@link ByteOutputContext}
   */
  private <T extends ByteTransferContext> void throwChannelErrorOnContexts(final ConcurrentMap<Integer, T> contexts,
                                                                           final Throwable cause) {
    for (final ByteTransferContext context : contexts.values()) {
      context.onChannelError(cause);
    }
  }
}
