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
package org.apache.nemo.runtime.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.nemo.runtime.master.resource.LambdaInboundHandler;
import org.apache.nemo.runtime.master.resource.NettyChannelInitializer;

import java.net.InetSocketAddress;
import java.util.Map;

/**
 * LambdaExecutor deployed on AWS Lambda.
 */
public class LambdaExecutor implements RequestHandler<Map<String, Object>, Context> {
  private Channel openChannel;
  private Bootstrap clientBootstrap;
  private EventLoopGroup clientWorkerGroup;

  private String serverAddr;
  private int serverPort;
  /**
   * Reads address and port from the netty server.
   * @param input
   * @param context
   * @return
   */
  @Override
  public Context handleRequest(final Map<String, Object> input, final Context context) {
    final String address = (String) input.get("address");
    final int port = (Integer) input.get("port");
    final String requestedAddr = "/" + address + ":" + port;
    System.out.println("Requested addr: " + requestedAddr);
    this.serverAddr = address;
    this.serverPort = port;

    // open channel
    this.clientWorkerGroup = new NioEventLoopGroup(1,
      new DefaultThreadFactory("hello" + "-ClientWorker"));
    this.clientBootstrap = new Bootstrap();
    this.clientBootstrap.group(clientWorkerGroup)
      .channel(NioSocketChannel.class)
      .handler(new NettyChannelInitializer(new LambdaInboundHandler()))
      .option(ChannelOption.SO_REUSEADDR, true)
      .option(ChannelOption.SO_KEEPALIVE, true);

    this.openChannel = channelOpen(input);

    return null;
  }

  /**
   * Parse input.
   * @param input
   * @return
   */
  private Channel channelOpen(final Map<String, Object> input) {
    // Connect to the LambdaMaster
    final ChannelFuture channelFuture;
    channelFuture = this.clientBootstrap.connect(new InetSocketAddress(this.serverAddr, this.serverPort));
    channelFuture.awaitUninterruptibly();
    assert channelFuture.isDone();
    if (!channelFuture.isSuccess()) {
      final StringBuilder sb = new StringBuilder("A connection failed at Source - ");
      sb.append(channelFuture.cause());
      throw new RuntimeException(sb.toString());
    }
    final Channel opendChannel = channelFuture.channel();
    return opendChannel;
  }
}
