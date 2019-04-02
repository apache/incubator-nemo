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
package org.apache.nemo.runtime.lambdaexecutor.datatransfer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.nemo.runtime.executor.common.datatransfer.ByteTransferContextSetupMessage;

import java.util.List;

/**
 * Encodes a control frame into bytes.
 *
 */
@ChannelHandler.Sharable
public final class LambdaControlFrameEncoder extends MessageToMessageEncoder<ByteTransferContextSetupMessage> {

  private static final int ZEROS_LENGTH = 5;
  private static final int BODY_LENGTH_LENGTH = Integer.BYTES;
  private static final ByteBuf ZEROS = Unpooled.directBuffer(ZEROS_LENGTH, ZEROS_LENGTH).writeZero(ZEROS_LENGTH);

  private final String localExecutorId;

  /**
   * Private constructor.
   */
  public LambdaControlFrameEncoder(final String localExecutorId) {
    this.localExecutorId = localExecutorId;
  }

  @Override
  protected void encode(final ChannelHandlerContext ctx,
                        final ByteTransferContextSetupMessage message,
                        final List out) {

    out.add(ZEROS.retain());


    final ByteBuf data = message.encode();

    //out.add(ctx.alloc().ioBuffer(BODY_LENGTH_LENGTH, BODY_LENGTH_LENGTH)
    //  .writeInt(frameBody.length));

    out.add(ctx.alloc().ioBuffer(BODY_LENGTH_LENGTH, BODY_LENGTH_LENGTH)
      .writeInt(data.readableBytes()));

    out.add(data);
  }
}
