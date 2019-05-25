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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import org.apache.nemo.common.Pair;
import org.apache.nemo.runtime.executor.common.datatransfer.ByteInputContext;
import org.apache.nemo.runtime.executor.common.datatransfer.ByteOutputContext;
import org.apache.nemo.runtime.executor.common.datatransfer.ContextManager;
import org.apache.nemo.runtime.executor.common.datatransfer.PipeTransferContextDescriptor;
import org.apache.nemo.runtime.executor.common.relayserverclient.RelayControlMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Bootstraps the server and connects to other servers on demand.
 */
public final class RelayServerClient {

  private static final Logger LOG = LoggerFactory.getLogger(RelayServerClient.class);
  private static final String CLIENT = "byte:client";

  private final EventLoopGroup clientGroup;
  private final Bootstrap clientBootstrap;

  private final Map<Pair<String, Integer>, ChannelFuture> channelMap;
  private final Map<Pair<String, Integer>, Boolean> channelExistingMap;

  private final String myRelayServerAddress;
  private final int myRelayServerPort;

  public RelayServerClient(final EventLoopGroup clientGroup,
                           final Bootstrap clientBootstrap,
                           final String myRelayServerAddress,
                           final int myRelayServerPort) {

    this.clientGroup = clientGroup;
    this.clientBootstrap = clientBootstrap;
    this.channelMap = new ConcurrentHashMap<>();
    this.channelExistingMap = new ConcurrentHashMap<>();
    this.myRelayServerAddress = myRelayServerAddress;
    this.myRelayServerPort = myRelayServerPort;

    //final ChannelFuture channelFuture = connectToRelayServer(relayServerAddress, relayServerPort);
    //this.relayServerChannel = channelFuture.channel();
  }

  public String getMyRelayServerAddress() {
    return myRelayServerAddress;
  }

  public int getMyRelayServerPort() {
    return myRelayServerPort;
  }

  public void registerTask(final Channel relayServerChannel,
                            final String edgeId,
                            final int taskIndex,
                            final boolean src) {
    // todo
    LOG.info("Registering task {}/{}/{} to {}", edgeId, taskIndex, src, relayServerChannel);
    final RelayControlMessage message = new RelayControlMessage(
      edgeId, taskIndex, src, RelayControlMessage.Type.REGISTER);
    relayServerChannel.writeAndFlush(message);
  }

  public void close() {
    final Future clientGroupCloseFuture = clientGroup.shutdownGracefully();
    clientGroupCloseFuture.awaitUninterruptibly();
  }

  public CompletableFuture<ByteOutputContext> newOutputContext(
    final String executorId,
    final PipeTransferContextDescriptor descriptor) {

      final CompletableFuture<ByteOutputContext> completableFuture = new CompletableFuture<>();

      final ChannelFuture channelFuture = connectToRelayServer(myRelayServerAddress, myRelayServerPort);
      while (!channelFuture.isSuccess()) {
        try {
          Thread.sleep(200);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

    LOG.info("Getting relay server channel for output context {}!!", descriptor);

    final Channel channel = channelFuture.channel();
    registerTask(channel, descriptor.getRuntimeEdgeId(), (int) descriptor.getSrcTaskIndex(), false);

    final ContextManager manager = channel.pipeline().get(ContextManager.class);
    LOG.info("Getting context manager!!!");
    completableFuture.complete(manager.newOutputContext(executorId, descriptor, true));
    return completableFuture;
  }

  public CompletableFuture<ByteInputContext> newInputContext(
    final String executorId,
    final PipeTransferContextDescriptor descriptor) {

      final CompletableFuture<ByteInputContext> completableFuture = new CompletableFuture<>();

      // 누구 relay server로 접근해야 하는지?
      // 내껄로 접근해야함
      final ChannelFuture channelFuture = connectToRelayServer(myRelayServerAddress, myRelayServerPort);
      while (!channelFuture.isSuccess()) {
        try {
          Thread.sleep(200);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

    LOG.info("Getting relay server channel for input context {}!!", descriptor);

    final Channel channel = channelFuture.channel();
    registerTask(channel, descriptor.getRuntimeEdgeId(), (int) descriptor.getDstTaskIndex(), true);

    final ContextManager manager = channel.pipeline().get(ContextManager.class);
    LOG.info("Getting context manager!!!");
    completableFuture.complete(manager.newInputContext(executorId, descriptor, true));
    return completableFuture;
  }



  public ChannelFuture connectToRelayServer(final String address, final int port) {

    final Pair<String, Integer> key = Pair.of(address, port);

    if (channelExistingMap.putIfAbsent(key, true) == null) {
      final InetSocketAddress socketAddress = new InetSocketAddress(address, port);
      final ChannelFuture connectFuture = clientBootstrap.connect(socketAddress);
      connectFuture.addListener(future -> {
        if (future.isSuccess()) {
          // Succeed to connect
          LOG.info("Connected to relay server {}:{}, {}", address, port, connectFuture.channel());
          return;
        }
        // Failed to connect (Not logging the cause here, which is not very useful)
        LOG.error("Failed to connect to relay server {}:{}", address, port);
      });

      channelMap.put(key, connectFuture);
      return connectFuture;
    } else {
      while (!channelMap.containsKey(key)) {
        try {
          Thread.sleep(200);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      return channelMap.get(key);
    }
  }
}
