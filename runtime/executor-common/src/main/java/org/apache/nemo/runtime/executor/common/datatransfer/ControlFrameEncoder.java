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
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;

/**
 * Encodes a control frame into bytes.
 *
 */
@ChannelHandler.Sharable
public final class ControlFrameEncoder extends MessageToMessageEncoder<TaskControlMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(ControlFrameEncoder.class.getName());

  // 1 byte: flag, 1 byte: broadcast, integer byte: transferindex or broadcast size, integer byte: body length
  public static final int ZEROS_LENGTH = 2 + Integer.BYTES;
  public static final int BODY_LENGTH_LENGTH = Integer.BYTES;
  public static final ByteBuf ZEROS = Unpooled.directBuffer(ZEROS_LENGTH, ZEROS_LENGTH).writeZero(ZEROS_LENGTH);


  /**
   * Private constructor.
   */
  @Inject
  public ControlFrameEncoder() {
  }

  @Override
  public void encode(final ChannelHandlerContext ctx,
                     final TaskControlMessage message,
                     final List out) {

    // final CompositeByteBuf cbb = ctx.alloc().compositeBuffer(3);

    final ByteBuf data = ctx.alloc().ioBuffer();
    final ByteBufOutputStream bos = new ByteBufOutputStream(data);
    message.encode(bos);
    try {
      bos.close();
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    final ByteBuf header = ctx.alloc().ioBuffer();
    header.writeZero(ZEROS_LENGTH);
    header.writeInt(data.readableBytes());

    // LOG.info("Encoding control message for type " + message.type + ", length " + data.readableBytes());

    out.add(header);
    out.add(data);

    /*
    cbb.addComponents(true, ZEROS.retain(),
      ctx.alloc().ioBuffer(BODY_LENGTH_LENGTH, BODY_LENGTH_LENGTH)
      .writeInt(data.readableBytes()),
      data);

    out.add(cbb);
    */

    /*
    out.add(ZEROS.retain());

    //out.add(ctx.alloc().ioBuffer(BODY_LENGTH_LENGTH, BODY_LENGTH_LENGTH)
    //  .writeInt(frameBody.length));

    out.add(ctx.alloc().ioBuffer(BODY_LENGTH_LENGTH, BODY_LENGTH_LENGTH)
      .writeInt(data.readableBytes()));

    out.add(data);
    */
  }
}
