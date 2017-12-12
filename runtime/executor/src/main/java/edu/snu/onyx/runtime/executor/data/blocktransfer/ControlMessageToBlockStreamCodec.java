/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.onyx.runtime.executor.data.blocktransfer;

import edu.snu.onyx.common.exception.UnsupportedBlockStoreException;
import edu.snu.onyx.common.ir.edge.executionproperty.DataStoreProperty;
import edu.snu.onyx.runtime.common.data.HashRange;
import edu.snu.onyx.runtime.common.comm.ControlMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Responses to control message by emitting a new {@link BlockStream},
 * and responses to {@link BlockStream} by emitting a new control message.
 *
 * <h3>Type of block transfer:</h3>
 * <ul>
 *   <li>In push-based transfer, the sender initiates block transfer and issues transfer id.</li>
 *   <li>In pull-based transfer, the receiver initiates block transfer and issues transfer id.</li>
 * </ul>
 *
 * @see BlockTransportChannelInitializer
 */
final class ControlMessageToBlockStreamCodec
    extends MessageToMessageCodec<ControlMessage.DataTransferControlMessage, BlockStream> {

  private static final Logger LOG = LoggerFactory.getLogger(ControlMessageToBlockStreamCodec.class);

  private final Map<Short, BlockInputStream> pullTransferIdToInputStream = new HashMap<>();
  private final Map<Short, BlockInputStream> pushTransferIdToInputStream = new HashMap<>();
  private final Map<Short, BlockOutputStream> pullTransferIdToOutputStream = new HashMap<>();
  private final Map<Short, BlockOutputStream> pushTransferIdToOutputStream = new HashMap<>();

  private final String localExecutorId;
  private SocketAddress localAddress;
  private SocketAddress remoteAddress;

  private short nextOutboundPullTransferId = 0;
  private short nextOutboundPushTransferId = 0;

  /**
   * Creates a {@link ControlMessageToBlockStreamCodec}.
   *
   * @param localExecutorId the id of this executor
   */
  ControlMessageToBlockStreamCodec(final String localExecutorId) {
    this.localExecutorId = localExecutorId;
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) {
    this.localAddress = ctx.channel().localAddress();
    this.remoteAddress = ctx.channel().remoteAddress();
    ctx.fireChannelActive();
  }

  /**
   * For an outbound {@link BlockStream}, which means {@link BlockTransfer} initiated a new transport context,
   * responds to it by emitting a new control message and registering the transport context to the internal map.
   *
   * @param ctx the {@link ChannelHandlerContext} which this handler belongs to
   * @param in  the {@link BlockStream}
   * @param out the {@link List} into which the created control message is added
   */
  @Override
  protected void encode(final ChannelHandlerContext ctx,
                        final BlockStream in,
                        final List out) {
    if (in instanceof BlockInputStream) {
      onOutboundPullRequest(ctx, (BlockInputStream) in, out);
    } else {
      onOutboundPushNotification(ctx, (BlockOutputStream) in, out);
    }
  }

  /**
   * Respond to {@link BlockInputStream} by emitting outbound pull request.
   *
   * @param ctx the {@link ChannelHandlerContext} which this handler belongs to
   * @param in  the {@link BlockInputStream}
   * @param out the {@link List} into which the created control message is added
   */
  private void onOutboundPullRequest(final ChannelHandlerContext ctx,
                                     final BlockInputStream in,
                                     final List out) {
    final short transferId = nextOutboundPullTransferId++;
    checkTransferIdAvailability(pullTransferIdToInputStream, ControlMessage.BlockTransferType.PULL, transferId);
    pullTransferIdToInputStream.put(transferId, in);
    emitControlMessage(ControlMessage.BlockTransferType.PULL, transferId, in, out);
    LOG.debug("Sending pull request {} from {}({}) to {}({}) for {} ({}, {} in {})",
        new Object[]{transferId, localExecutorId, localAddress, in.getRemoteExecutorId(), remoteAddress,
            in.getBlockId(), in.getRuntimeEdgeId(), in.getHashRange().toString(),
            in.getBlockStore().get().toString()});
  }

  /**
   * Respond to {@link BlockOutputStream} by emitting outbound push notification.
   *
   * @param ctx the {@link ChannelHandlerContext} which this handler belongs to
   * @param in  the {@link BlockOutputStream}
   * @param out the {@link List} into which the created control message is added
   */
  private void onOutboundPushNotification(final ChannelHandlerContext ctx,
                                          final BlockOutputStream in,
                                          final List out) {
    final short transferId = nextOutboundPushTransferId++;
    checkTransferIdAvailability(pushTransferIdToOutputStream, ControlMessage.BlockTransferType.PUSH, transferId);
    pushTransferIdToOutputStream.put(transferId, in);
    in.setTransferIdAndChannel(ControlMessage.BlockTransferType.PUSH, transferId, ctx.channel());
    emitControlMessage(ControlMessage.BlockTransferType.PUSH, transferId, in, out);
    LOG.debug("Sending push notification {} from {}({}) to {}({}) for {} ({}, {})",
        new Object[]{transferId, localExecutorId, localAddress, in.getRemoteExecutorId(), remoteAddress,
            in.getBlockId(), in.getRuntimeEdgeId(), in.getHashRange().toString()});
  }

  /**
   * For an inbound control message (pull request or push notification), which initiates a transport context, responds
   * to it by registering the transport context to the internal mapping, and emitting a new {@link BlockStream},
   * which will be handled by {@link BlockTransfer}.
   *
   * @param ctx the {@link ChannelHandlerContext} which this handler belongs to
   * @param in  the inbound control message
   * @param out the {@link List} into which the created {@link BlockStream} is added
   */
  @Override
  protected void decode(final ChannelHandlerContext ctx,
                        final ControlMessage.DataTransferControlMessage in,
                        final List out) {
    if (in.getType() == ControlMessage.BlockTransferType.PULL) {
      onInboundPullRequest(ctx, in, out);
    } else {
      onInboundPushNotification(ctx, in, out);
    }
  }

  /**
   * Respond to pull request by other executors by emitting a new {@link BlockOutputStream}.
   *
   * @param ctx the {@link ChannelHandlerContext} which this handler belongs to
   * @param in  the control message
   * @param out the {@link List} into which the created {@link BlockOutputStream} is added
   */
  private void onInboundPullRequest(final ChannelHandlerContext ctx,
                                    final ControlMessage.DataTransferControlMessage in,
                                    final List out) {
    final short transferId = (short) in.getTransferId();
    final HashRange hashRange = in.hasStartRangeInclusive() && in.hasEndRangeExclusive()
        ? HashRange.of(in.getStartRangeInclusive(), in.getEndRangeExclusive()) : HashRange.all();
    final BlockOutputStream outputStream = new BlockOutputStream(in.getControlMessageSourceId(),
        in.getEncodePartialBlock(), Optional.of(convertBlockStore(in.getBlockStore())), in.getBlockId(),
        in.getRuntimeEdgeId(), hashRange);
    pullTransferIdToOutputStream.put(transferId, outputStream);
    outputStream.setTransferIdAndChannel(ControlMessage.BlockTransferType.PULL, transferId, ctx.channel());
    out.add(outputStream);
    LOG.debug("Received pull request {} from {}({}) to {}({}) for {} ({}, {} in {})",
        new Object[]{transferId, in.getControlMessageSourceId(), remoteAddress, localExecutorId, localAddress,
            in.getBlockId(), in.getRuntimeEdgeId(), outputStream.getHashRange().toString(),
            outputStream.getBlockStore().get().toString()});
  }

  /**
   * Respond to push notification by other executors by emitting a new {@link BlockInputStream}.
   *
   * @param ctx the {@link ChannelHandlerContext} which this handler belongs to
   * @param in  the control message
   * @param out the {@link List} into which the created {@link BlockInputStream} is added
   */
  private void onInboundPushNotification(final ChannelHandlerContext ctx,
                                         final ControlMessage.DataTransferControlMessage in,
                                         final List out) {
    final short transferId = (short) in.getTransferId();
    final HashRange hashRange = in.hasStartRangeInclusive() && in.hasEndRangeExclusive()
        ? HashRange.of(in.getStartRangeInclusive(), in.getEndRangeExclusive()) : HashRange.all();
    final BlockInputStream inputStream = new BlockInputStream(in.getControlMessageSourceId(),
        in.getEncodePartialBlock(), Optional.empty(), in.getBlockId(), in.getRuntimeEdgeId(), hashRange);
    pushTransferIdToInputStream.put(transferId, inputStream);
    out.add(inputStream);
    LOG.debug("Received push notification {} from {}({}) to {}({}) for {} ({}, {})",
        new Object[]{transferId, in.getControlMessageSourceId(), remoteAddress, localExecutorId, localAddress,
            in.getBlockId(), in.getRuntimeEdgeId(), inputStream.getHashRange().toString()});
  }

  /**
   * Check whether the transfer id is not being used.
   *
   * @param map          the map with transfer id as key
   * @param transferType the transfer type
   * @param transferId   the transfer id
   */
  private void checkTransferIdAvailability(final Map<Short, ?> map,
                                           final ControlMessage.BlockTransferType transferType,
                                           final short transferId) {
    if (map.get(transferId) != null) {
      LOG.error("Transfer id {}:{} to {} is still being used, ignoring",
          new Object[]{transferType, transferId, remoteAddress});
    }
  }

  /**
   * Builds and emits control message.
   *
   * @param transferType the transfer type
   * @param transferId   the transfer id
   * @param in           {@link BlockInputStream} or {@link BlockOutputStream}
   * @param out          the {@link List} into which the created control message is added
   */
  private void emitControlMessage(final ControlMessage.BlockTransferType transferType,
                                  final short transferId,
                                  final BlockStream in,
                                  final List out) {
    final ControlMessage.DataTransferControlMessage.Builder controlMessageBuilder
        = ControlMessage.DataTransferControlMessage.newBuilder()
        .setControlMessageSourceId(localExecutorId)
        .setType(transferType)
        .setTransferId(transferId)
        .setEncodePartialBlock(in.isEncodePartialBlockEnabled())
        .setBlockId(in.getBlockId())
        .setRuntimeEdgeId(in.getRuntimeEdgeId());
    if (in.getBlockStore().isPresent()) {
      controlMessageBuilder.setBlockStore(convertBlockStore(in.getBlockStore().get()));
    }
    if (!in.getHashRange().isAll()) {
      controlMessageBuilder
          .setStartRangeInclusive(in.getHashRange().rangeStartInclusive())
          .setEndRangeExclusive(in.getHashRange().rangeEndExclusive());
    }
    out.add(controlMessageBuilder.build());
  }

  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
    for (final BlockInputStream stream : pullTransferIdToInputStream.values()) {
      stream.onExceptionCaught(cause);
    }
    for (final BlockInputStream stream : pushTransferIdToInputStream.values()) {
      stream.onExceptionCaught(cause);
    }
    for (final BlockOutputStream stream : pullTransferIdToOutputStream.values()) {
      stream.onExceptionCaught(cause);
    }
    for (final BlockOutputStream stream : pushTransferIdToOutputStream.values()) {
      stream.onExceptionCaught(cause);
    }
    ctx.fireExceptionCaught(cause);
  }

  /**
   * Gets {@code pullTransferIdToInputStream}.
   *
   * @return {@code pullTransferIdToInputStream}
   */
  Map<Short, BlockInputStream> getPullTransferIdToInputStream() {
    return pullTransferIdToInputStream;
  }

  /**
   * Gets {@code pushTransferIdToInputStream}.
   *
   * @return {@code pushTransferIdToInputStream}
   */
  Map<Short, BlockInputStream> getPushTransferIdToInputStream() {
    return pushTransferIdToInputStream;
  }

  /**
   * Gets {@code pullTransferIdToOutputStream}.
   *
   * @return {@code pullTransferIdToOutputStream}
   */
  Map<Short, BlockOutputStream> getPullTransferIdToOutputStream() {
    return pullTransferIdToOutputStream;
  }

  /**
   * Gets {@code pushTransferIdToOutputStream}.
   *
   * @return {@code pushTransferIdToOutputStream}
   */
  Map<Short, BlockOutputStream> getPushTransferIdToOutputStream() {
    return pushTransferIdToOutputStream;
  }

  private static ControlMessage.BlockStore convertBlockStore(
      final DataStoreProperty.Value blockStore) {
    switch (blockStore) {
      case MemoryStore:
        return ControlMessage.BlockStore.MEMORY;
      case SerializedMemoryStore:
        return ControlMessage.BlockStore.SER_MEMORY;
      case LocalFileStore:
        return ControlMessage.BlockStore.LOCAL_FILE;
      case GlusterFileStore:
        return ControlMessage.BlockStore.REMOTE_FILE;
      default:
        throw new UnsupportedBlockStoreException(new Exception(blockStore + " is not supported."));
    }
  }

  private static DataStoreProperty.Value convertBlockStore(
      final ControlMessage.BlockStore blockStoreType) {
    switch (blockStoreType) {
      case MEMORY:
        return DataStoreProperty.Value.MemoryStore;
      case SER_MEMORY:
        return DataStoreProperty.Value.SerializedMemoryStore;
      case LOCAL_FILE:
        return DataStoreProperty.Value.LocalFileStore;
      case REMOTE_FILE:
        return DataStoreProperty.Value.GlusterFileStore;
      default:
        throw new UnsupportedBlockStoreException(new Exception("This block store is not yet supported"));
    }
  }
}
