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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import org.apache.nemo.runtime.executor.common.datatransfer.*;
import org.apache.nemo.runtime.executor.common.relayserverclient.RelayControlFrame;
import org.apache.nemo.runtime.executor.common.relayserverclient.RelayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
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

  private final ConcurrentMap<Integer, ByteInputContext> inputContexts = new ConcurrentHashMap<>();
  private final ConcurrentMap<Integer, ByteOutputContext> outputContexts = new ConcurrentHashMap<>();
  private final AtomicInteger nextOutputTransferIndex = new AtomicInteger(0);
  private final AtomicInteger nextInputTransferIndex = new AtomicInteger(0);

  private final AckScheduledService ackScheduledService;

  private final Map<TransferKey, Integer> taskTransferIndexMap;
  private final boolean isRelayServerChannel;
  private final ExecutorService channelExecutorService;
  private final RelayServerClient relayServerClient;

  public LambdaContextManager(
    final ExecutorService channelExecutorService,
    final ChannelGroup channelGroup,
    final String localExecutorId,
    final Channel channel,
    final AckScheduledService ackScheduledService,
    final Map<TransferKey, Integer> taskTransferIndexMap,
    final boolean isRelayServerChannel,
    final RelayServerClient relayServerClient) {
    LOG.info("New lambda context manager: {} / {}", localExecutorId, channel);
    this.channelExecutorService = channelExecutorService;
    this.channelGroup = channelGroup;
    this.localExecutorId = localExecutorId;
    this.channel = channel;
    this.ackScheduledService = ackScheduledService;
    this.taskTransferIndexMap = taskTransferIndexMap;
    this.isRelayServerChannel = isRelayServerChannel;
    this.relayServerClient = relayServerClient;

    LOG.info("Transfer index map: {}", taskTransferIndexMap);
  }

  @Override
  public Channel getChannel() {
    return channel;
  }

  @Override
  public boolean isRelayServerContext() {
    return isRelayServerChannel;
  }

  @Override
  public ByteInputContext getInputContext(ByteTransferContextSetupMessage.ByteTransferDataDirection dataDirection, int transferIndex) {
    return inputContexts.get(transferIndex);
  }

  @Override
  public ByteInputContext newInputContext(String executorId, byte[] contextDescriptor, boolean isPipe) {
    final PipeTransferContextDescriptor cd = PipeTransferContextDescriptor.decode(contextDescriptor);
    final TransferKey key =  new TransferKey(cd.getRuntimeEdgeId(),
      (int) cd.getSrcTaskIndex(), (int) cd.getDstTaskIndex(), false);

    final int transferIndex = taskTransferIndexMap.get(key);
    LOG.info("Input context for {}/{}, index: {}", cd.getRuntimeEdgeId(), (int) cd.getSrcTaskIndex(), transferIndex);

    return newContext(inputContexts, transferIndex,
      ByteTransferContextSetupMessage.ByteTransferDataDirection.INITIATOR_RECEIVES_DATA,
      contextId -> new StreamRemoteByteInputContext(executorId, contextId, contextDescriptor, this,
        ackScheduledService.ackService),
      executorId, isPipe, null);
  }

  @Override
  protected void channelRead0(final ChannelHandlerContext ctx, final ByteTransferContextSetupMessage message)
      throws Exception {
    // TODO: handle scaleout messages!
    setRemoteExecutorId(message.getInitiatorExecutorId());
    final ByteTransferContextSetupMessage.ByteTransferDataDirection
      dataDirection = message.getDataDirection();
    final int transferIndex = message.getTransferIndex();
    final boolean isPipe = message.getIsPipe();
    final ByteTransferContext.ContextId contextId =
      new ByteTransferContext.ContextId(remoteExecutorId, localExecutorId, dataDirection, transferIndex, isPipe);
    final byte[] contextDescriptor = message.getContextDescriptor();

    switch (message.getMessageType()) {
      case ACK_FROM_UPSTREAM: {
        final ByteInputContext context = inputContexts.get(transferIndex);
        LOG.info("ACK_FROM_UPSTREAM: {}, {}", transferIndex, inputContexts);
        context.receivePendingAck();
        break;
      }
      case PENDING_FOR_SCALEOUT_VM: {
        // send channel request
        final String relayServerAddress = message.getRelayServerAddress();
        final int relayServerPort = message.getRelayServerPort();

        // THIS means that the up/downstream operator are going to sf/vm
        // We should connect to the relay server!
        LOG.info("SCALEOUT pending to {}/{} relay server... {}, {}",
          relayServerAddress, relayServerPort, transferIndex, outputContexts);

        final ByteOutputContext outputContext = outputContexts.get(transferIndex);
        outputContext.pending(ByteOutputContext.SendDataTo.SF, relayServerAddress, relayServerPort);
        break;
      }
      case PENDING_FOR_SCALEIN_VM: {

        throw new RuntimeException("Not supported yet");

        /*
        LOG.info("SCALEIN pending {}", transferIndex);
        final ByteOutputContext outputContext = outputContexts.get(transferIndex);
        outputContext.pending(ByteOutputContext.SendDataTo.VM, null, 1);
        break;
        */
      }
      case STOP_INPUT_FOR_SCALEOUT: {
        // connect to relay server
        final String relayServerAddress = message.getRelayServerAddress();
        final int relayServerPort = message.getRelayServerPort();
        LOG.info("Connecting to relay server for input {}/{}", relayServerAddress, relayServerPort);

        final PipeTransferContextDescriptor cd = PipeTransferContextDescriptor.decode(contextDescriptor);

        channelExecutorService.execute(() -> {
          final ChannelFuture channelFuture = relayServerClient.connectToRelayServer(relayServerAddress, relayServerPort);
          while (!channelFuture.isDone()) {
            try {
              Thread.sleep(500);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }

          final Channel relayServerChannel = channelFuture.channel();

          relayServerClient.registerTask(relayServerChannel,
            cd.getRuntimeEdgeId(), (int) cd.getDstTaskIndex(), true);

          LOG.info("Sending ack to the input context");
          // ACK to the original channel
          final ByteTransferContextSetupMessage ackMessage =
            new ByteTransferContextSetupMessage(contextId.getInitiatorExecutorId(),
              contextId.getTransferIndex(),
              contextId.getDataDirection(),
              contextDescriptor,
              contextId.isPipe(),
              ByteTransferContextSetupMessage.MessageType.ACK_FROM_UPSTREAM);
          channel.writeAndFlush(ackMessage);

        });
        break;
      }
      case STOP_INPUT_FOR_SCALEIN: {
        break;
      }
      default: {
        throw new RuntimeException("Unsupported type: " + message.getMessageType());
      }
    }
  }

  @Override
  public void onContextExpired(final ByteTransferContext context) {
    // TODO: Send end message!
    LOG.warn("Do nothing in context expired");
    //throw new UnsupportedOperationException("unsupported");
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
                                               final int transferIndex,
                                               final ByteTransferContextSetupMessage.ByteTransferDataDirection dataDirection,
                                               final Function<ByteTransferContext.ContextId, T> contextGenerator,
                                               final String executorId,
                                               final boolean isPipe,
                                               final String relayDst) {
    setRemoteExecutorId(executorId);
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

    if (isRelayServerChannel) {
      channel.writeAndFlush(new RelayControlFrame(relayDst, message)).addListener(context.getChannelWriteListener());
    } else {
      channel.writeAndFlush(message).addListener(context.getChannelWriteListener());
    }
    return context;
  }

  @Override
  public ByteOutputContext newOutputContext(final String executorId,
                                            final PipeTransferContextDescriptor descriptor,
                                            final boolean isPipe) {
    LOG.info("LambdaContextManager: {}", executorId);
    final TransferKey key =  new TransferKey(descriptor.getRuntimeEdgeId(),
      (int) descriptor.getSrcTaskIndex(), (int) descriptor.getDstTaskIndex(), true);

    final int transferIndex = taskTransferIndexMap.get(key);
    LOG.info("Output context for {}/{}, index: {}", descriptor.getRuntimeEdgeId(), (int) descriptor.getDstTaskIndex(), transferIndex);

    final byte[] encodedDescriptor = descriptor.encode();
    if (isRelayServerChannel) {
      final String relayDst = RelayUtils.createId(descriptor.getRuntimeEdgeId(), (int) descriptor.getDstTaskIndex(), false);
      return newContext(outputContexts, transferIndex,
        ByteTransferContextSetupMessage.ByteTransferDataDirection.INITIATOR_SENDS_DATA,
        contextId -> new LambdaRemoteByteOutputContext(executorId, contextId, encodedDescriptor, this, relayDst),
        executorId, isPipe, relayDst);
    } else {
      return newContext(outputContexts, transferIndex,
        ByteTransferContextSetupMessage.ByteTransferDataDirection.INITIATOR_SENDS_DATA,
        contextId -> new LambdaRemoteByteOutputContext(executorId, contextId, encodedDescriptor, this, null),
        executorId, isPipe, null);
    }
  }

  private void setRemoteExecutorId(final String executorId) {
    if (remoteExecutorId == null) {
      remoteExecutorId = executorId;
    } else if (!executorId.equals(remoteExecutorId)) {
      LOG.warn(String.format("Wrong ContextManager: (%s != %s)", executorId, remoteExecutorId));
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
    throwChannelErrorOnContexts(outputContexts, cause);
  }

  private <T extends ByteTransferContext> void throwChannelErrorOnContexts(final ConcurrentMap<Integer, T> contexts,
                                                                           final Throwable cause) {
    for (final ByteTransferContext context : contexts.values()) {
      context.onChannelError(cause);
    }
  }
}
