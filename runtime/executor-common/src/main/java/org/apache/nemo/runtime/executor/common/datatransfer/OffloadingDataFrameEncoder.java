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
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.util.Recycler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.io.IOException;
import java.util.List;

/**
 * Encodes a data frame into bytes.
 *
 */
@ChannelHandler.Sharable
public final class OffloadingDataFrameEncoder extends MessageToMessageEncoder<OffloadingDataFrameEncoder.DataFrame> {
  private static final Logger LOG = LoggerFactory.getLogger(OffloadingDataFrameEncoder.class.getName());


  @Inject
  public OffloadingDataFrameEncoder() {
  }

  @Override
  public void encode(final ChannelHandlerContext ctx, final DataFrame in, final List out) {

    final ByteBuf buf = ctx.alloc().ioBuffer();
    buf.writeByte(in.type.ordinal());

    switch (in.type) {
      case OFFLOAD_NORMAL_OUTPUT:  {
        try {
          final ByteBufOutputStream bos = new ByteBufOutputStream(buf);
          bos.writeInt(in.pipeIndices.get(0));
        } catch (final Exception e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
        break;
      }
      case OFFLOAD_BROADCAST_OUTPUT: {
        try {
          final ByteBufOutputStream bos = new ByteBufOutputStream(buf);
          bos.writeInt(in.pipeIndices.size());
          for (final int index : in.pipeIndices) {
            bos.writeInt(index);
          }
        } catch (final Exception e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
        break;
      }
      case DEOFFLOAD_DONE: {
        final ByteBufOutputStream bos = new ByteBufOutputStream(buf);
        try {
          bos.writeUTF(in.taskId);
        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
        break;
      }
      default:
        throw new RuntimeException("invalid type " + in.type);
    }

    // encode body
    if (in.body != null) {
      buf.writeBytes((ByteBuf) in.body);
      out.add(buf);
      //out.add(in.body);
    } else {
      out.add(buf);
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
      protected DataFrame newObject(final Handle handle) {
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
    public DataFrameEncoder.DataType type;
    @Nullable
    public Object body;
    public long length;
    public boolean opensSubStream;
    public boolean closesContext;
    public boolean stopContext;
    public String taskId;

    public static DataFrame newInstance(DataFrameEncoder.DataType type,
                                        final String taskId) {
      final DataFrame dataFrame = RECYCLER.get();
      dataFrame.taskId = taskId;
      dataFrame.type = type;
      dataFrame.body = null;
      dataFrame.pipeIndices = null;
      return dataFrame;
    }

    public static DataFrame newInstance(final List<Integer> indices,
                                        @Nullable final Object body,
                                        final long length) {

      final DataFrame dataFrame = RECYCLER.get();
      if (indices.size() < 1) {
        throw new RuntimeException("Invalid task index");
      }

      if (indices.size() == 1) {
        dataFrame.type = DataFrameEncoder.DataType.OFFLOAD_NORMAL_OUTPUT;
      } else {
        dataFrame.type = DataFrameEncoder.DataType.OFFLOAD_BROADCAST_OUTPUT;
      }

      dataFrame.pipeIndices = indices;
      dataFrame.body = body;
      dataFrame.length = length;
      dataFrame.opensSubStream = true;
      dataFrame.closesContext = false;
      dataFrame.stopContext = false;
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
