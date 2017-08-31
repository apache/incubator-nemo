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
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.util.Recycler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.List;

/**
 * Encodes a data frame into bytes.
 *
 * @see FrameDecoder
 */
@ChannelHandler.Sharable
final class DataFrameEncoder extends MessageToMessageEncoder<DataFrameEncoder.DataFrame> {

  private static final Logger LOG = LoggerFactory.getLogger(DataFrameEncoder.class);

  static final int TYPE_AND_TRANSFERID_LENGTH = Short.BYTES + Short.BYTES;
  // the length of a frame body (not the entire frame) is stored in 4 bytes
  static final int BODYLENGTH_LENGTH = Integer.BYTES;
  static final int HEADER_LENGTH = TYPE_AND_TRANSFERID_LENGTH + BODYLENGTH_LENGTH;

  // the maximum length of a frame body. 2**32 - 1
  static final long LENGTH_MAX = 4294967295L;

  @Inject
  private DataFrameEncoder() {
  }

  @Override
  protected void encode(final ChannelHandlerContext ctx, final DataFrame in, final List<Object> out) {
    // encode header
    final ByteBuf header = ctx.alloc().ioBuffer(HEADER_LENGTH, HEADER_LENGTH);
    final short type;
    final boolean isFetch = in.type == ControlMessage.PartitionTransferType.FETCH;
    if (isFetch) {
      if (in.isLastFrame) {
        type = FrameDecoder.FETCH_LASTFRAME;
      } else {
        type = FrameDecoder.FETCH_INTERMEDIATE_FRAME;
      }
    } else {
      if (in.isLastFrame) {
        type = FrameDecoder.SEND_LASTFRAME;
      } else {
        type = FrameDecoder.SEND_INTERMEDIATE_FRAME;
      }
    }
    header.writeShort(type);
    header.writeShort(in.transferId);

    // in.length should not exceed the range of unsigned int
    assert (in.length <= LENGTH_MAX);
    header.writeInt((int) in.length);

    out.add(header);

    // encode body
    if (in.body != null) {
      out.add(in.body);
    }

    // recycle DataFrame object
    in.recycle();

    // a transport context is closed. remove from map.
    if (in.isLastFrame) {
      final ControlMessageToPartitionStreamCodec duplexHandler
          = ctx.channel().pipeline().get(ControlMessageToPartitionStreamCodec.class);
      (isFetch ? duplexHandler.getFetchTransferIdToOutputStream() : duplexHandler.getSendTransferIdToOutputStream())
          .remove(in.transferId);
      LOG.debug("Closing transport {}:{}, where the partition sender is {} and the receiver is {}", new Object[]{
          isFetch ? "fetch" : "send", in.transferId, ctx.channel().localAddress(), ctx.channel().remoteAddress()});
    }
  }

  /**
   * Data frame representation.
   */
  static final class DataFrame {

    private static final Recycler<DataFrame> RECYCLER = new Recycler<DataFrame>() {
      @Override
      protected DataFrame newObject(final Recycler.Handle handle) {
        return new DataFrame(handle);
      }
    };

    private final Recycler.Handle handle;

    /**
     * Creates a {@link DataFrame}.
     *
     * @param handle the recycler handle
     */
    private DataFrame(final Recycler.Handle handle) {
      this.handle = handle;
    }

    private ControlMessage.PartitionTransferType type;
    private boolean isLastFrame;
    private short transferId;
    private long length;
    @Nullable
    private Object body;

    /**
     * Creates a {@link DataFrame}.
     *
     * @param type        the transfer type, namely fetch or send
     * @param isLastFrame whether or not this frame is the last frame of a data message
     * @param transferId  the transfer id
     * @param length      the length of the body, in bytes
     * @param body        the body
     * @return the {@link DataFrame} object
     */
    static DataFrame newInstance(final ControlMessage.PartitionTransferType type,
                                 final boolean isLastFrame,
                                 final short transferId,
                                 final long length,
                                 @Nullable final Object body) {
      final DataFrame dataFrame = RECYCLER.get();
      dataFrame.type = type;
      dataFrame.isLastFrame = isLastFrame;
      dataFrame.transferId = transferId;
      dataFrame.length = length;
      dataFrame.body = body;
      return dataFrame;
    }

    /**
     * Recycles this object.
     */
    void recycle() {
      body = null;
      RECYCLER.recycle(this, handle);
    }
  }
}
