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

import edu.snu.vortex.runtime.common.comm.ControlMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;

import java.util.List;
import java.util.Map;

/**
 * A placeholder implementation.
 */
final class ControlMessageToPartitionStreamCodec extends MessageToMessageCodec<ControlMessage, Object> {
  /**
   * Creates a {@link ControlMessageToPartitionStreamCodec}.
   * @param localExecutorId the id of this executor
   */
  ControlMessageToPartitionStreamCodec(final String localExecutorId) {
  }

  /**
   * For an outbound {@link PartitionInputStream} or "partition output stream" (not implemented yet), which indicates
   * "partition transfer" (not implemented yet) decided to initiate a new transport context, responds to it by emitting
   * a new control message.
   *
   * @param ctx the netty channel handler context
   * @param in  the outbound {@link PartitionInputStream} or "partition output stream" (not implemented yet)
   * @param out the control message
   */
  @Override
  protected void encode(final ChannelHandlerContext ctx, final Object in, final List<Object> out) {
    // not implemented
  }

  /**
   * For inbound control messages ("fetch request" or "send notification"), which initiates transport context,
   * responds to it by emitting a new {@link PartitionInputStream} or "partition output stream" (not implemented yet),
   * which will be handled by "partition transfer" (not implemented yet).
   *
   * @param ctx the netty channel handler context
   * @param in  the inbound control message
   * @param out the {@link List} to which the created {@link PartitionInputStream} or "partition output stream" object
   *            will be added
   */
  @Override
  protected void decode(final ChannelHandlerContext ctx, final ControlMessage in, final List<Object> out) {
    // not implemented
  }

  Map<Short, PartitionInputStream> getFetchTransferIdToInputStream() {
    return null;
  }

  Map<Short, PartitionInputStream> getSendTransferIdToInputStream() {
    return null;
  }

  Map<Short, Object> getFetchTransferIdToOutputStream() {
    return null;
  }

  Map<Short, Object> getSendTransferIdToOutputStream() {
    return null;
  }
}
