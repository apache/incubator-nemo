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
package edu.snu.vortex.runtime.executor.data.partitiontransfer;

import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.exception.UnsupportedPartitionStoreException;
import edu.snu.vortex.runtime.executor.data.HashRange;
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
 *   <li>In send-based transfer, the sender initiates partition transfer and issues transfer id.</li>
 *   <li>In fetch-based transfer, the receiver initiates partition transfer and issues transfer id.</li>
 * </ul>
 *
 * @see PartitionTransportChannelInitializer
 */
final class ControlMessageToPartitionStreamCodec
    extends MessageToMessageCodec<ControlMessage.DataTransferControlMessage, PartitionStream> {

  private static final Logger LOG = LoggerFactory.getLogger(ControlMessageToPartitionStreamCodec.class);

  private final Map<Short, PartitionInputStream> fetchTransferIdToInputStream = new HashMap<>();
  private final Map<Short, PartitionInputStream> sendTransferIdToInputStream = new HashMap<>();
  private final Map<Short, PartitionOutputStream> fetchTransferIdToOutputStream = new HashMap<>();
  private final Map<Short, PartitionOutputStream> sendTransferIdToOutputStream = new HashMap<>();

  private final String localExecutorId;
  private SocketAddress localAddress;
  private SocketAddress remoteAddress;

  private short nextOutboundFetchTransferId = 0;
  private short nextOutboundSendTransferId = 0;

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
                        final List<Object> out) {
    if (in instanceof PartitionInputStream) {
      onOutboundFetchRequest(ctx, (PartitionInputStream) in, out);
    } else {
      onOutboundSendNotification(ctx, (PartitionOutputStream) in, out);
    }
  }

  /**
   * Respond to {@link PartitionInputStream} by emitting outbound fetch request.
   *
   * @param ctx the {@link ChannelHandlerContext} which this handler belongs to
   * @param in  the {@link PartitionInputStream}
   * @param out the {@link List} into which the created control message is added
   */
  private void onOutboundFetchRequest(final ChannelHandlerContext ctx,
                                      final PartitionInputStream in,
                                      final List<Object> out) {
    final short transferId = nextOutboundFetchTransferId++;
    checkTransferIdAvailability(fetchTransferIdToInputStream, ControlMessage.PartitionTransferType.FETCH, transferId);
    fetchTransferIdToInputStream.put(transferId, in);
    emitControlMessage(ControlMessage.PartitionTransferType.FETCH, transferId, in, out);
    LOG.debug("Sending fetch request {} from {}({}) to {}({}) for {} ({}, {} in {})",
        new Object[]{transferId, localExecutorId, localAddress, in.getRemoteExecutorId(), remoteAddress,
        in.getPartitionId(), in.getRuntimeEdgeId(), in.getHashRange().toString(),
        in.getPartitionStore().get().toString()});
  }

  /**
   * Respond to {@link PartitionOutputStream} by emitting outbound send notification.
   *
   * @param ctx the {@link ChannelHandlerContext} which this handler belongs to
   * @param in  the {@link PartitionOutputStream}
   * @param out the {@link List} into which the created control message is added
   */
  private void onOutboundSendNotification(final ChannelHandlerContext ctx,
                                          final PartitionOutputStream in,
                                          final List<Object> out) {
    final short transferId = nextOutboundSendTransferId++;
    checkTransferIdAvailability(sendTransferIdToOutputStream, ControlMessage.PartitionTransferType.SEND, transferId);
    sendTransferIdToOutputStream.put(transferId, in);
    in.setTransferIdAndChannel(ControlMessage.PartitionTransferType.SEND, transferId, ctx.channel());
    emitControlMessage(ControlMessage.PartitionTransferType.SEND, transferId, in, out);
    LOG.debug("Sending send notification {} from {}({}) to {}({}) for {} ({}, {})",
        new Object[]{transferId, localExecutorId, localAddress, in.getRemoteExecutorId(), remoteAddress,
        in.getPartitionId(), in.getRuntimeEdgeId(), in.getHashRange().toString()});
  }

  /**
   * For an inbound control message (fetch request or send notification), which initiates a transport context, responds
   * to it by registering the transport context to the internal mapping, and emitting a new {@link PartitionStream},
   * which will be handled by {@link PartitionTransfer}.
   *
   * @param ctx the {@link ChannelHandlerContext} which this handler belongs to
   * @param in  the inbound control message
   * @param out the {@link List} into which the created {@link PartitionStream} is added
   */
  @Override
  protected void decode(final ChannelHandlerContext ctx,
                        final ControlMessage.DataTransferControlMessage in,
                        final List<Object> out) {
    if (in.getType() == ControlMessage.PartitionTransferType.FETCH) {
      onInboundFetchRequest(ctx, in, out);
    } else {
      onInboundSendNotification(ctx, in, out);
    }
  }

  /**
   * Respond to fetch request by other executors by emitting a new {@link PartitionOutputStream}.
   *
   * @param ctx the {@link ChannelHandlerContext} which this handler belongs to
   * @param in  the control message
   * @param out the {@link List} into which the created {@link PartitionOutputStream} is added
   */
  private void onInboundFetchRequest(final ChannelHandlerContext ctx,
                                     final ControlMessage.DataTransferControlMessage in,
                                     final List<Object> out) {
    final short transferId = (short) in.getTransferId();
    final HashRange hashRange = in.hasStartRangeInclusive() && in.hasEndRangeExclusive()
        ? HashRange.of(in.getStartRangeInclusive(), in.getEndRangeExclusive()) : HashRange.all();
    final PartitionOutputStream outputStream = new PartitionOutputStream(in.getControlMessageSourceId(),
        in.getEncodePartialPartition(), Optional.of(convertPartitionStore(in.getPartitionStore())), in.getPartitionId(),
        in.getRuntimeEdgeId(), hashRange);
    fetchTransferIdToOutputStream.put(transferId, outputStream);
    outputStream.setTransferIdAndChannel(ControlMessage.PartitionTransferType.FETCH, transferId, ctx.channel());
    out.add(outputStream);
    LOG.debug("Received fetch request {} from {}({}) to {}({}) for {} ({}, {} in {})",
        new Object[]{transferId, in.getControlMessageSourceId(), remoteAddress, localExecutorId, localAddress,
        in.getPartitionId(), in.getRuntimeEdgeId(), outputStream.getHashRange().toString(),
        outputStream.getPartitionStore().get().toString()});
  }

  /**
   * Respond to send notification by other executors by emitting a new {@link PartitionInputStream}.
   *
   * @param ctx the {@link ChannelHandlerContext} which this handler belongs to
   * @param in  the control message
   * @param out the {@link List} into which the created {@link PartitionInputStream} is added
   */
  private void onInboundSendNotification(final ChannelHandlerContext ctx,
                                         final ControlMessage.DataTransferControlMessage in,
                                         final List<Object> out) {
    final short transferId = (short) in.getTransferId();
    final HashRange hashRange = in.hasStartRangeInclusive() && in.hasEndRangeExclusive()
        ? HashRange.of(in.getStartRangeInclusive(), in.getEndRangeExclusive()) : HashRange.all();
    final PartitionInputStream inputStream = new PartitionInputStream(in.getControlMessageSourceId(),
        in.getEncodePartialPartition(), Optional.empty(), in.getPartitionId(), in.getRuntimeEdgeId(), hashRange);
    sendTransferIdToInputStream.put(transferId, inputStream);
    out.add(inputStream);
    LOG.debug("Received send notification {} from {}({}) to {}({}) for {} ({}, {})",
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
                                           final ControlMessage.PartitionTransferType transferType,
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
  private void emitControlMessage(final ControlMessage.PartitionTransferType transferType,
                                  final short transferId,
                                  final PartitionStream in,
                                  final List<Object> out) {
    final ControlMessage.DataTransferControlMessage.Builder controlMessageBuilder
        = ControlMessage.DataTransferControlMessage.newBuilder()
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
    for (final PartitionInputStream stream : fetchTransferIdToInputStream.values()) {
      stream.onExceptionCaught(cause);
    }
    for (final PartitionInputStream stream : sendTransferIdToInputStream.values()) {
      stream.onExceptionCaught(cause);
    }
    for (final PartitionOutputStream stream : fetchTransferIdToOutputStream.values()) {
      stream.onExceptionCaught(cause);
    }
    for (final PartitionOutputStream stream : sendTransferIdToOutputStream.values()) {
      stream.onExceptionCaught(cause);
    }
    ctx.fireExceptionCaught(cause);
  }

  /**
   * Gets {@code fetchTransferIdToInputStream}.
   *
   * @return {@code fetchTransferIdToInputStream}
   */
  Map<Short, PartitionInputStream> getFetchTransferIdToInputStream() {
    return fetchTransferIdToInputStream;
  }

  /**
   * Gets {@code sendTransferIdToInputStream}.
   *
   * @return {@code sendTransferIdToInputStream}
   */
  Map<Short, PartitionInputStream> getSendTransferIdToInputStream() {
    return sendTransferIdToInputStream;
  }

  /**
   * Gets {@code fetchTransferIdToOutputStream}.
   *
   * @return {@code fetchTransferIdToOutputStream}
   */
  Map<Short, PartitionOutputStream> getFetchTransferIdToOutputStream() {
    return fetchTransferIdToOutputStream;
  }

  /**
   * Gets {@code sendTransferIdToOutputStream}.
   *
   * @return {@code sendTransferIdToOutputStream}
   */
  Map<Short, PartitionOutputStream> getSendTransferIdToOutputStream() {
    return sendTransferIdToOutputStream;
  }

  private static ControlMessage.PartitionStore convertPartitionStore(final Attribute partitionStore) {
    switch (partitionStore) {
      case Memory:
        return ControlMessage.PartitionStore.MEMORY;
      case LocalFile:
        return ControlMessage.PartitionStore.LOCAL_FILE;
      case RemoteFile:
        return ControlMessage.PartitionStore.REMOTE_FILE;
      default:
        throw new UnsupportedPartitionStoreException(new Exception(partitionStore + " is not supported."));
    }
  }

  private static Attribute convertPartitionStore(final ControlMessage.PartitionStore partitionStoreType) {
    switch (partitionStoreType) {
      case MEMORY:
        return Attribute.Memory;
      case LOCAL_FILE:
        return Attribute.LocalFile;
      case REMOTE_FILE:
        return Attribute.RemoteFile;
      default:
        throw new UnsupportedPartitionStoreException(new Exception("This partition store is not yet supported"));
    }
  }
}
