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
package edu.snu.onyx.runtime.executor.data.partitiontransfer;

import edu.snu.onyx.runtime.exception.UnsupportedPartitionStoreException;
import edu.snu.onyx.runtime.executor.data.*;
import edu.snu.onyx.runtime.executor.data.stores.*;
import edu.snu.onyx.runtime.executor.grpc.ExecutorPartition;
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
 * Responses to control message by emitting a new {@link PartitionStream},
 * and responses to {@link PartitionStream} by emitting a new control message.
 *
 * <h3>Type of partition transfer:</h3>
 * <ul>
 *   <li>In push-based transfer, the sender initiates partition transfer and issues transfer id.</li>
 *   <li>In pull-based transfer, the receiver initiates partition transfer and issues transfer id.</li>
 * </ul>
 *
 * @see PartitionTransportChannelInitializer
 */
final class ControlMessageToPartitionStreamCodec
    extends MessageToMessageCodec<ExecutorPartition.DataTransferControlMessage, PartitionStream> {

  private static final Logger LOG = LoggerFactory.getLogger(ControlMessageToPartitionStreamCodec.class);

  private final Map<Short, PartitionInputStream> pullTransferIdToInputStream = new HashMap<>();
  private final Map<Short, PartitionInputStream> pushTransferIdToInputStream = new HashMap<>();
  private final Map<Short, PartitionOutputStream> pullTransferIdToOutputStream = new HashMap<>();
  private final Map<Short, PartitionOutputStream> pushTransferIdToOutputStream = new HashMap<>();

  private final String localExecutorId;
  private SocketAddress localAddress;
  private SocketAddress remoteAddress;

  private short nextOutboundPullTransferId = 0;
  private short nextOutboundPushTransferId = 0;

  /**
   * Creates a {@link ControlMessageToPartitionStreamCodec}.
   *
   * @param localExecutorId the id of this executor
   */
  ControlMessageToPartitionStreamCodec(final String localExecutorId) {
    this.localExecutorId = localExecutorId;
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) {
    this.localAddress = ctx.channel().localAddress();
    this.remoteAddress = ctx.channel().remoteAddress();
    ctx.fireChannelActive();
  }

  /**
   * For an outbound {@link PartitionStream}, which means {@link PartitionTransfer} initiated a new transport context,
   * responds to it by emitting a new control message and registering the transport context to the internal map.
   *
   * @param ctx the {@link ChannelHandlerContext} which this handler belongs to
   * @param in  the {@link PartitionStream}
   * @param out the {@link List} into which the created control message is added
   */
  @Override
  protected void encode(final ChannelHandlerContext ctx,
                        final PartitionStream in,
                        final List out) {
    if (in instanceof PartitionInputStream) {
      onOutboundPullRequest(ctx, (PartitionInputStream) in, out);
    } else {
      onOutboundPushNotification(ctx, (PartitionOutputStream) in, out);
    }
  }

  /**
   * Respond to {@link PartitionInputStream} by emitting outbound pull request.
   *
   * @param ctx the {@link ChannelHandlerContext} which this handler belongs to
   * @param in  the {@link PartitionInputStream}
   * @param out the {@link List} into which the created control message is added
   */
  private void onOutboundPullRequest(final ChannelHandlerContext ctx,
                                     final PartitionInputStream in,
                                     final List out) {
    final short transferId = nextOutboundPullTransferId++;
    checkTransferIdAvailability(pullTransferIdToInputStream, ExecutorPartition.PartitionTransferType.PULL, transferId);
    pullTransferIdToInputStream.put(transferId, in);
    emitControlMessage(ExecutorPartition.PartitionTransferType.PULL, transferId, in, out);
    LOG.debug("Sending pull request {} from {}({}) to {}({}) for {} ({}, {} in {})",
        new Object[]{transferId, localExecutorId, localAddress, in.getRemoteExecutorId(), remoteAddress,
            in.getPartitionId(), in.getRuntimeEdgeId(), in.getHashRange().toString(),
            in.getPartitionStore().get().toString()});
  }

  /**
   * Respond to {@link PartitionOutputStream} by emitting outbound push notification.
   *
   * @param ctx the {@link ChannelHandlerContext} which this handler belongs to
   * @param in  the {@link PartitionOutputStream}
   * @param out the {@link List} into which the created control message is added
   */
  private void onOutboundPushNotification(final ChannelHandlerContext ctx,
                                          final PartitionOutputStream in,
                                          final List out) {
    final short transferId = nextOutboundPushTransferId++;
    checkTransferIdAvailability(pushTransferIdToOutputStream, ExecutorPartition.PartitionTransferType.PUSH, transferId);
    pushTransferIdToOutputStream.put(transferId, in);
    in.setTransferIdAndChannel(ExecutorPartition.PartitionTransferType.PUSH, transferId, ctx.channel());
    emitControlMessage(ExecutorPartition.PartitionTransferType.PUSH, transferId, in, out);
    LOG.debug("Sending push notification {} from {}({}) to {}({}) for {} ({}, {})",
        new Object[]{transferId, localExecutorId, localAddress, in.getRemoteExecutorId(), remoteAddress,
            in.getPartitionId(), in.getRuntimeEdgeId(), in.getHashRange().toString()});
  }

  /**
   * For an inbound control message (pull request or push notification), which initiates a transport context, responds
   * to it by registering the transport context to the internal mapping, and emitting a new {@link PartitionStream},
   * which will be handled by {@link PartitionTransfer}.
   *
   * @param ctx the {@link ChannelHandlerContext} which this handler belongs to
   * @param in  the inbound control message
   * @param out the {@link List} into which the created {@link PartitionStream} is added
   */
  @Override
  protected void decode(final ChannelHandlerContext ctx,
                        final ExecutorPartition.DataTransferControlMessage in,
                        final List out) {
    if (in.getType() == ExecutorPartition.PartitionTransferType.PULL) {
      onInboundPullRequest(ctx, in, out);
    } else {
      onInboundPushNotification(ctx, in, out);
    }
  }

  /**
   * Respond to pull request by other executors by emitting a new {@link PartitionOutputStream}.
   *
   * @param ctx the {@link ChannelHandlerContext} which this handler belongs to
   * @param in  the control message
   * @param out the {@link List} into which the created {@link PartitionOutputStream} is added
   */
  private void onInboundPullRequest(final ChannelHandlerContext ctx,
                                    final ExecutorPartition.DataTransferControlMessage in,
                                    final List out) {
    final short transferId = (short) in.getTransferId();
    final HashRange hashRange = in.hasStartRangeInclusive() && in.hasEndRangeExclusive()
        ? HashRange.of(in.getStartRangeInclusive(), in.getEndRangeExclusive()) : HashRange.all();
    final PartitionOutputStream outputStream = new PartitionOutputStream(in.getControlMessageSourceId(),
        in.getEncodePartialPartition(), Optional.of(convertPartitionStore(in.getPartitionStore())), in.getPartitionId(),
        in.getRuntimeEdgeId(), hashRange);
    pullTransferIdToOutputStream.put(transferId, outputStream);
    outputStream.setTransferIdAndChannel(ExecutorPartition.PartitionTransferType.PULL, transferId, ctx.channel());
    out.add(outputStream);
    LOG.debug("Received pull request {} from {}({}) to {}({}) for {} ({}, {} in {})",
        new Object[]{transferId, in.getControlMessageSourceId(), remoteAddress, localExecutorId, localAddress,
            in.getPartitionId(), in.getRuntimeEdgeId(), outputStream.getHashRange().toString(),
            outputStream.getPartitionStore().get().toString()});
  }

  /**
   * Respond to push notification by other executors by emitting a new {@link PartitionInputStream}.
   *
   * @param ctx the {@link ChannelHandlerContext} which this handler belongs to
   * @param in  the control message
   * @param out the {@link List} into which the created {@link PartitionInputStream} is added
   */
  private void onInboundPushNotification(final ChannelHandlerContext ctx,
                                         final ExecutorPartition.DataTransferControlMessage in,
                                         final List out) {
    final short transferId = (short) in.getTransferId();
    final HashRange hashRange = in.hasStartRangeInclusive() && in.hasEndRangeExclusive()
        ? HashRange.of(in.getStartRangeInclusive(), in.getEndRangeExclusive()) : HashRange.all();
    final PartitionInputStream inputStream = new PartitionInputStream(in.getControlMessageSourceId(),
        in.getEncodePartialPartition(), Optional.empty(), in.getPartitionId(), in.getRuntimeEdgeId(), hashRange);
    pushTransferIdToInputStream.put(transferId, inputStream);
    out.add(inputStream);
    LOG.debug("Received push notification {} from {}({}) to {}({}) for {} ({}, {})",
        new Object[]{transferId, in.getControlMessageSourceId(), remoteAddress, localExecutorId, localAddress,
            in.getPartitionId(), in.getRuntimeEdgeId(), inputStream.getHashRange().toString()});
  }

  /**
   * Check whether the transfer id is not being used.
   *
   * @param map           the map with transfer id as key
   * @param transferType  the transfer type
   * @param transferId    the transfer id
   */
  private void checkTransferIdAvailability(final Map<Short, ?> map,
                                           final ExecutorPartition.PartitionTransferType transferType,
                                           final short transferId) {
    if (map.get(transferId) != null) {
      LOG.error("Transfer id {}:{} to {} is still being used, ignoring",
          new Object[]{transferType, transferId, remoteAddress});
    }
  }

  /**
   * Builds and emits control message.
   *
   * @param transferType  the transfer type
   * @param transferId    the transfer id
   * @param in            {@link PartitionInputStream} or {@link PartitionOutputStream}
   * @param out           the {@link List} into which the created control message is added
   */
  private void emitControlMessage(final ExecutorPartition.PartitionTransferType transferType,
                                  final short transferId,
                                  final PartitionStream in,
                                  final List out) {
    final ExecutorPartition.DataTransferControlMessage.Builder controlMessageBuilder
        = ExecutorPartition.DataTransferControlMessage.newBuilder()
        .setControlMessageSourceId(localExecutorId)
        .setType(transferType)
        .setTransferId(transferId)
        .setEncodePartialPartition(in.isEncodePartialPartitionEnabled())
        .setPartitionId(in.getPartitionId())
        .setRuntimeEdgeId(in.getRuntimeEdgeId());
    if (in.getPartitionStore().isPresent()) {
      controlMessageBuilder.setPartitionStore(convertPartitionStore(in.getPartitionStore().get()));
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
    for (final PartitionInputStream stream : pullTransferIdToInputStream.values()) {
      stream.onExceptionCaught(cause);
    }
    for (final PartitionInputStream stream : pushTransferIdToInputStream.values()) {
      stream.onExceptionCaught(cause);
    }
    for (final PartitionOutputStream stream : pullTransferIdToOutputStream.values()) {
      stream.onExceptionCaught(cause);
    }
    for (final PartitionOutputStream stream : pushTransferIdToOutputStream.values()) {
      stream.onExceptionCaught(cause);
    }
    ctx.fireExceptionCaught(cause);
  }

  /**
   * Gets {@code pullTransferIdToInputStream}.
   *
   * @return {@code pullTransferIdToInputStream}
   */
  Map<Short, PartitionInputStream> getPullTransferIdToInputStream() {
    return pullTransferIdToInputStream;
  }

  /**
   * Gets {@code pushTransferIdToInputStream}.
   *
   * @return {@code pushTransferIdToInputStream}
   */
  Map<Short, PartitionInputStream> getPushTransferIdToInputStream() {
    return pushTransferIdToInputStream;
  }

  /**
   * Gets {@code pullTransferIdToOutputStream}.
   *
   * @return {@code pullTransferIdToOutputStream}
   */
  Map<Short, PartitionOutputStream> getPullTransferIdToOutputStream() {
    return pullTransferIdToOutputStream;
  }

  /**
   * Gets {@code pushTransferIdToOutputStream}.
   *
   * @return {@code pushTransferIdToOutputStream}
   */
  Map<Short, PartitionOutputStream> getPushTransferIdToOutputStream() {
    return pushTransferIdToOutputStream;
  }

  private static ExecutorPartition.PartitionStore convertPartitionStore(
      final Class<? extends PartitionStore> partitionStore) {
    switch (partitionStore.getSimpleName()) {
      case MemoryStore.SIMPLE_NAME:
        return ExecutorPartition.PartitionStore.MEMORY;
      case SerializedMemoryStore.SIMPLE_NAME:
        return ExecutorPartition.PartitionStore.SER_MEMORY;
      case LocalFileStore.SIMPLE_NAME:
        return ExecutorPartition.PartitionStore.LOCAL_FILE;
      case GlusterFileStore.SIMPLE_NAME:
        return ExecutorPartition.PartitionStore.REMOTE_FILE;
      default:
        throw new UnsupportedPartitionStoreException(new Exception(partitionStore + " is not supported."));
    }
  }

  private static Class<? extends PartitionStore> convertPartitionStore(
      final ExecutorPartition.PartitionStore partitionStoreType) {
    switch (partitionStoreType) {
      case MEMORY:
        return MemoryStore.class;
      case SER_MEMORY:
        return SerializedMemoryStore.class;
      case LOCAL_FILE:
        return LocalFileStore.class;
      case REMOTE_FILE:
        return GlusterFileStore.class;
      default:
        throw new UnsupportedPartitionStoreException(new Exception("This partition store is not yet supported"));
    }
  }
}
