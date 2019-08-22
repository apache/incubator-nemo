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
package org.apache.nemo.runtime.master.resource;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * LambdaEventCoder used in NettyChannelInitializer when first receiving connection.
 */
public final class LambdaEventCoder {
  private static final Logger LOG = LoggerFactory.getLogger(LambdaEventCoder.class.getName());

  /**
   * LambdaEventEncoder.
   */
  public static final class LambdaEventEncoder extends MessageToMessageEncoder<LambdaEvent> {

    @Override
    protected void encode(final ChannelHandlerContext ctx,
                          final LambdaEvent msg, final List<Object> out) throws Exception {
      System.out.println("LambdaEventEncoder->encode LambdaEvent.getBytes().toString: " + msg.getBytes().toString());

      ByteBuf byteBuf = Unpooled.copiedBuffer(SerializationUtils.serialize(msg));
      out.add(byteBuf);
      System.out.println("bytebuf readableBytes " + byteBuf.readableBytes());
      for(byte i : msg.getBytes()) {
        System.out.print(i);
      }
/*      ByteBuf byteBuf = ctx.alloc().buffer(5, msg.getLen() + 5);
      byteBuf.writeInt(msg.getLen());
      byteBuf.writeByte((byte)msg.getType().ordinal());
      byteBuf.writeBytes(msg.getBytes());

      out.add(byteBuf);
      System.out.println("LambdaEvent encoded " + byteBuf.toString());
 */
    }
  }

  /**
   * LambdaEventDecoder.
   */
  public static final class LambdaEventDecoder extends MessageToMessageDecoder<ByteBuf> {

    @Override
    protected void decode(final ChannelHandlerContext ctx,
                          final ByteBuf buf, final List<Object> out) throws Exception {
      System.out.println("bytebuf.readableBytes " + buf.readableBytes());
      try {
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);
        LambdaEvent lambdaEvent = (LambdaEvent) SerializationUtils.deserialize(bytes);
        out.add(lambdaEvent);

        System.out.println("DEBUG " + lambdaEvent.getLen());
        for(byte i : lambdaEvent.getBytes()) {
          System.out.print(i);
        }

/*        final int len = buf.readInt();
        System.out.println("LambdaEventDecoder len: " + len);
        final LambdaEvent.Type type = LambdaEvent.Type.values()[buf.readByte()];
        System.out.println("LambdaEvent Type: " + type);
        byte[] src = new byte[buf.readableBytes()];
        System.out.println("Readable bytes" + buf.readableBytes());
        buf.readBytes(src);
        final LambdaEvent lambdaEvent = new LambdaEvent(type, src, len);
        System.out.println("src: " + src.toString());

        out.add(lambdaEvent);
        System.out.println("Decoded LambdaEvent.toString: "
          + lambdaEvent.toString() + " event bytes: "+ lambdaEvent.getBytes().toString());
 */
      } catch (final ArrayIndexOutOfBoundsException e) {
        e.printStackTrace();
        throw e;
      }
    }
  }
}
