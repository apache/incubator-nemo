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

import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

/**
 * This class initializes socket channel for text messages.
 */
public final class NettyChannelInitializer
  extends ChannelInitializer<SocketChannel> {

  private final ChannelInboundHandlerAdapter inboundHandlerAdapter;

  public NettyChannelInitializer(final ChannelInboundHandlerAdapter channelInboundHandlerAdapter) {
    this.inboundHandlerAdapter = channelInboundHandlerAdapter;
  }

  /**
   * Initializes the socket channel with lineBasedFrame decoder for text messages.
   * @param ch
   * @throws Exception
   */
  @Override
  protected void initChannel(final SocketChannel ch) {
    System.out.println("NettyChannelInitializer inits SocketChannel " + ch.toString());
    ch.pipeline()
      .addLast("frameDecoder", new LengthFieldBasedFrameDecoder(
        Integer.MAX_VALUE, 0, 4, 0, 4))
      .addLast("frameEncoder", new LengthFieldPrepender(4))
      .addLast("decoder", new LambdaEventCoder.LambdaEventDecoder())
      .addLast("encoder", new LambdaEventCoder.LambdaEventEncoder())
      .addLast("handler", inboundHandlerAdapter);
  }
}

