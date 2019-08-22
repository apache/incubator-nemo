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
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
      System.out.println("LambdaEventEncoder->encode " + msg.getType());
      final ByteBuf buf = ctx.alloc().buffer(4 + msg.getLen());
      LOG.info("Encoded bytes: " + msg.getLen() + 8);
      buf.writeInt(msg.getLen());
      buf.writeInt(msg.getType().ordinal());
      buf.writeBytes(msg.getBytes(), 0, msg.getLen());

      LOG.info("Encode " + msg.getType().name() + ", size: " + buf.readableBytes());
      out.add(buf);
    }
  }

  /**
   * LambdaEventDecoder.
   */
  public static final class LambdaEventDecoder extends MessageToMessageDecoder<ByteBuf> {

    @Override
    protected void decode(final ChannelHandlerContext ctx,
                          final ByteBuf msg, final List<Object> out) throws Exception {
      System.out.println("LambdaEventDecoder->decode " + msg.toString());
      System.out.println("Size: " + msg.readableBytes());
      System.out.println("Size: " + msg.readableBytes());
      try {
        final int len = msg.readInt();
        final LambdaEvent.Type type = LambdaEvent.Type.values()[msg.readInt()];
        System.out.println("Decode message; " + type.name()
          + ", size: " + msg.readableBytes() + "byte array length " + len);

        byte[] arr = new byte[msg.readableBytes()];
        msg.readBytes(arr);
        System.out.println(arr.toString());

        out.add(new LambdaEvent(type, arr, len));
      } catch (final ArrayIndexOutOfBoundsException e) {
        e.printStackTrace();
        throw e;
      }
    }
  }
}

