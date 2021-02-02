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
package org.apache.nemo.runtime.executor.common.datatransfer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.util.Recycler;
import org.apache.nemo.offloading.common.OffloadingOutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.List;

/**
 * Encodes a data frame into bytes.
 *
 */
@ChannelHandler.Sharable
public final class DataFrameEncoder extends MessageToMessageEncoder<DataFrameEncoder.DataFrame> {
  private static final Logger LOG = LoggerFactory.getLogger(DataFrameEncoder.class.getName());

  private static final int TRANSFER_INDEX_LENGTH = Integer.BYTES;
  private static final int BODY_LENGTH_LENGTH = Integer.BYTES;
  public static final int HEADER_LENGTH = Byte.BYTES  + Byte.BYTES + TRANSFER_INDEX_LENGTH + BODY_LENGTH_LENGTH;

  public enum DataType {
    NORMAL,
    BROADCAST,
    OFFLOAD_NORMAL_OUTPUT,
    OFFLOAD_BROADCAST_OUTPUT
  }

  // the maximum length of a frame body. 2**32 - 1
  static final long LENGTH_MAX = 4294967295L;

  @Inject
  public DataFrameEncoder() {
  }

  public ByteBuf encode(final ChannelHandlerContext ctx, final DataFrame in) {
    // encode header
    byte flags = (byte) 0;

    if (in.stopContext) {
      flags |= (byte) (1 << 4);
    }

    flags |= (byte) (1 << 3);
    // in.contextId.getDataDirection() == ByteTransferContextSetupMessage.ByteTransferDataDirection.INITIATOR_RECEIVES_DATA
    flags |= (byte) (1 << 2);
    if (in.opensSubStream) {
      flags |= (byte) (1 << 1);
    }
    if (in.closesContext) {
      flags |= (byte) (1 << 0);
    }

    ByteBuf header;

    switch (in.type) {
      case NORMAL:
        header = ctx.alloc().ioBuffer(HEADER_LENGTH, HEADER_LENGTH);
        header.writeByte(flags);
        header.writeByte(in.type.ordinal());
        header.writeInt(in.pipeIndices.get(0));
        break;
      case BROADCAST: {
        header = ctx.alloc().ioBuffer(HEADER_LENGTH
          + in.pipeIndices.size() * Integer.BYTES, HEADER_LENGTH
          + in.pipeIndices.size() * Integer.BYTES);

        header.writeByte(flags);
        header.writeByte(in.type.ordinal());
        header.writeInt(in.pipeIndices.size());
        for (int pipeIndex : in.pipeIndices) {
          header.writeInt(pipeIndex);
        }
        break;
      }
      default:
        throw new RuntimeException("invalid type " + in.type);
    }

    // in.length should not exceed the range of unsigned int
    assert (in.length <= LENGTH_MAX);
    header.writeInt((int) in.length);

    // encode body
    final CompositeByteBuf cbb = ctx.alloc().compositeBuffer(2);
    cbb.addComponents(true, header, (ByteBuf) in.body);

    // recycle DataFrame object
    in.recycle();

    return cbb;
  }

  @Override
  public void encode(final ChannelHandlerContext ctx, final DataFrame in, final List out) {
    // encode header
    byte flags = (byte) 0;

    if (in.stopContext) {
      flags |= (byte) (1 << 4);
    }

    flags |= (byte) (1 << 3);
    // in.contextId.getDataDirection() == ByteTransferContextSetupMessage.ByteTransferDataDirection.INITIATOR_RECEIVES_DATA
    flags |= (byte) (1 << 2);
    if (in.opensSubStream) {
      flags |= (byte) (1 << 1);
    }
    if (in.closesContext) {
      flags |= (byte) (1 << 0);
    }

    ByteBuf header;

    switch (in.type) {
      case NORMAL:
        header = ctx.alloc().ioBuffer(HEADER_LENGTH, HEADER_LENGTH);
        header.writeByte(flags);
        header.writeByte(in.type.ordinal());
        header.writeInt(in.pipeIndices.get(0));
        break;
      case BROADCAST: {
        header = ctx.alloc().ioBuffer(HEADER_LENGTH
          + in.pipeIndices.size() * Integer.BYTES, HEADER_LENGTH
          + in.pipeIndices.size() * Integer.BYTES);

        header.writeByte(flags);
        header.writeByte(in.type.ordinal());
        header.writeInt(in.pipeIndices.size());
        for (int pipeIndex : in.pipeIndices) {
          header.writeInt(pipeIndex);
        }
        break;
      }
      default:
        throw new RuntimeException("invalid type " + in.type);
    }

    // LOG.info("Encoding {}->{}, header: {}, body: {}",
    //  in.srcTask, in.taskIndices, header.readableBytes(),
    //  ((ByteBuf) in.body).readableBytes());

    // in.length should not exceed the range of unsigned int
    assert (in.length <= LENGTH_MAX);
    header.writeInt((int) in.length);

    // encode body
    if (in.body != null) {
      final CompositeByteBuf cbb = ctx.alloc().compositeBuffer(2);
      cbb.addComponents(true, header, (ByteBuf) in.body);
      out.add(cbb);
      //out.add(in.body);
    } else {
      out.add(header);
    }

    // recycle DataFrame object
    in.recycle();
  }

  /**
   * Data frame representation.
   */
  public static final class DataFrame {

    private static final Recycler<DataFrame> RECYCLER = new Recycler<DataFrame>() {
      @Override
      protected DataFrame newObject(final Recycler.Handle handle) {
        return new DataFrame(handle);
      }
    };

    /**
     * Creates a {@link DataFrame}.
     *
     * @param handle the recycler handle
     */
    private DataFrame(final Recycler.Handle handle) {
      this.handle = handle;
    }

    public final Recycler.Handle handle;
    public List<Integer> pipeIndices;
    public DataType type;
    @Nullable
    public Object body;
    public long length;
    public boolean opensSubStream;
    public boolean closesContext;
    public boolean stopContext;
    public String srcTaskId;
    public String edgeId;
    public List<String> dstTaskIds;


    /**
     * For broadcast!!
     */
    public static DataFrame newInstance(final List<Integer> pipeIndicies,
                                        @Nullable final Object body,
                                        final long length,
                                        final boolean opensSubStream) {
      final DataFrame dataFrame = RECYCLER.get();
      if (pipeIndicies.size() < 1) {
        throw new RuntimeException("Invalid task index");
      }

      if (pipeIndicies.size() == 1) {
        dataFrame.type = DataType.NORMAL;
      } else {
        dataFrame.type = DataType.BROADCAST;
      }

      dataFrame.pipeIndices = pipeIndicies;
      dataFrame.body = body;
      dataFrame.length = length;
      dataFrame.opensSubStream = opensSubStream;
      dataFrame.closesContext = false;
      dataFrame.stopContext = false;
      return dataFrame;
    }

    /**
     * Creates a {@link DataFrame} to supply content to sub-stream.
     *
     * @param contextId   the context id
     * @param body        the body or {@code null}
     * @param length      the length of the body, in bytes
     * @param opensSubStream whether this frame opens a new sub-stream or not
     * @return the {@link DataFrame} object
     */
    @Deprecated
    public static DataFrame newInstance(final ByteTransferContext.ContextId contextId,
                                 @Nullable final Object body,
                                 final long length,
                                 final boolean opensSubStream) {
      final DataFrame dataFrame = RECYCLER.get();
      // dataFrame.contextId = contextId;
      // dataFrame.isBroadcast = false;
      // dataFrame.contextIds = null;
      dataFrame.body = body;
      dataFrame.length = length;
      dataFrame.opensSubStream = opensSubStream;
      dataFrame.closesContext = false;
      dataFrame.stopContext = false;
      return dataFrame;
    }

    /**
     * Creates a {@link DataFrame} to close the whole context.
     * @param contextId   the context id
     * @return the {@link DataFrame} object
     */
    @Deprecated
    public static DataFrame newInstance(final ByteTransferContext.ContextId contextId) {
      final DataFrame dataFrame = RECYCLER.get();
      // dataFrame.contextId = contextId;
      // dataFrame.isBroadcast = false;
      // dataFrame.contextIds = null;
      dataFrame.body = null;
      dataFrame.length = 0;
      dataFrame.opensSubStream = false;
      dataFrame.closesContext = true;
      dataFrame.stopContext = false;
      return dataFrame;
    }

    @Deprecated
    public static DataFrame newInstanceForStop(final ByteTransferContext.ContextId contextId) {
      final DataFrame dataFrame = RECYCLER.get();
      // dataFrame.contextId = contextId;
      // dataFrame.isBroadcast = false;
      // dataFrame.contextIds = null;
      dataFrame.body = null;
      dataFrame.length = 0;
      dataFrame.opensSubStream = false;
      dataFrame.closesContext = false;
      dataFrame.stopContext = true;
      return dataFrame;
    }
    /**
     * Recycles this object.
     */
    public void recycle() {
      body = null;
      handle.recycle(this);
    }
  }
}
