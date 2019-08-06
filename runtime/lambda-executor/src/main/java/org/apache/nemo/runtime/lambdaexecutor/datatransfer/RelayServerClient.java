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
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Future;
import org.apache.nemo.common.Pair;
import org.apache.nemo.runtime.executor.common.datatransfer.ByteInputContext;
import org.apache.nemo.runtime.executor.common.datatransfer.ByteOutputContext;
import org.apache.nemo.runtime.executor.common.datatransfer.ContextManager;
import org.apache.nemo.runtime.executor.common.datatransfer.PipeTransferContextDescriptor;
import org.apache.nemo.runtime.executor.common.relayserverclient.RelayControlMessage;
import org.apache.nemo.runtime.executor.common.relayserverclient.RelayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
  private final Map<Channel, Set<String>> registerTaskMap;

  private final String myRelayServerAddress;
  private final int myRelayServerPort;
  private final Map<String, Pair<String, Integer>> relayServerInfo;

  private final RendevousServerClient rendevousServerClient;

  public RelayServerClient(final EventLoopGroup clientGroup,
                           final Bootstrap clientBootstrap,
                           final String myRelayServerAddress,
                           final int myRelayServerPort,
                           final Map<String, Pair<String, Integer>> relayServerInfo,
                           final RendevousServerClient rendevousServerClient) {

    this.clientGroup = clientGroup;
    this.clientBootstrap = clientBootstrap;
    this.channelMap = new ConcurrentHashMap<>();
    this.channelExistingMap = new ConcurrentHashMap<>();
    this.registerTaskMap = new ConcurrentHashMap<>();
    this.myRelayServerAddress = myRelayServerAddress;
    this.myRelayServerPort = myRelayServerPort;
    this.relayServerInfo = relayServerInfo;
    this.rendevousServerClient = rendevousServerClient;

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
    final String key = String.format("%s#%d#%s", edgeId, taskIndex, String.valueOf(src));

    registerTaskMap.putIfAbsent(relayServerChannel, new HashSet<>());

    final Set<String> registeredTasks = registerTaskMap.get(relayServerChannel);

    synchronized (registeredTasks) {
      if (!registeredTasks.contains(key)) {
        registeredTasks.add(key);

        //LOG.info("Registering task {}/{}/{} to {}", edgeId, taskIndex, src, relayServerChannel);
        final RelayControlMessage message = new RelayControlMessage(
          edgeId, taskIndex, src, RelayControlMessage.Type.REGISTER);
        relayServerChannel.writeAndFlush(message);
      }
    }
  }

  public void close() {
    final Future clientGroupCloseFuture = clientGroup.shutdownGracefully();
    clientGroupCloseFuture.awaitUninterruptibly();
  }

  public Channel getRelayServerChannel(final String dstExecutorId) {

    final Pair<String, Integer> info = relayServerInfo.get(dstExecutorId);
    final ChannelFuture channelFuture = connectToRelayServer(info.left(), info.right());

    //LOG.info("Get relay server channel {} / {}", dstExecutorId, info);

    while (!channelFuture.isSuccess()) {
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    return channelFuture.channel();
  }

  private Channel getMyRelayServerChannel() {
    final ChannelFuture channelFuture = connectToRelayServer(myRelayServerAddress, myRelayServerPort);
    while (!channelFuture.isSuccess()) {
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    final Channel channel = channelFuture.channel();
    return channel;
  }

  public CompletableFuture<ByteOutputContext> newOutputContext(
    final String srcExecutorId,
    final String dstExecutorId,
    final PipeTransferContextDescriptor descriptor) {

    final Channel channel = getRelayServerChannel(dstExecutorId);
    final CompletableFuture<ByteOutputContext> completableFuture = new CompletableFuture<>();

    //LOG.info("Getting relay output server channel for remote executor {}->{}, {}/{}->{}, channel {}!!",
    //  srcExecutorId, dstExecutorId,
    //  descriptor.getRuntimeEdgeId(), descriptor.getSrcTaskIndex(), descriptor.getDstTaskIndex(),
    //  channel);

    // ㄴㅐ꺼에다 register 하기
    final Channel myChannel = getMyRelayServerChannel();
    registerTask(myChannel,
      descriptor.getRuntimeEdgeId(), (int) descriptor.getSrcTaskIndex(), false);

    // 여기서 rendevous server에 등록하기
    final String relayDst = RelayUtils.createId(descriptor.getRuntimeEdgeId(), (int) descriptor.getDstTaskIndex(), true);
    final String response = rendevousServerClient.requestAddress(relayDst);

    final ContextManager manager = channel.pipeline().get(ContextManager.class);
    //LOG.info("Getting context manager!!!");
    completableFuture.complete(manager.newOutputContext(dstExecutorId, descriptor, true));
    return completableFuture;
  }

  public CompletableFuture<ByteInputContext> newInputContext(
    final String srcExecutorId,
    final String dstExecutorId,
    final PipeTransferContextDescriptor descriptor) {

      final CompletableFuture<ByteInputContext> completableFuture = new CompletableFuture<>();

      // 누구 relay server로 접근해야 하는지?
      // 내껄로 접근해야함
    final Channel channel = getMyRelayServerChannel();
    //LOG.info("Getting relay input server channel for remote executor {}->{}, {}/{}->{}!!, " +
    //    "channel: {}",
    //  srcExecutorId, dstExecutorId,
    //  descriptor.getRuntimeEdgeId(), descriptor.getSrcTaskIndex(), descriptor.getDstTaskIndex(),
    //  channel);

    registerTask(channel, descriptor.getRuntimeEdgeId(), (int) descriptor.getDstTaskIndex(), true);


    // rendevous server에 등록
    final String relayDst = RelayUtils.createId(descriptor.getRuntimeEdgeId(), (int) descriptor.getDstTaskIndex(), true);
    rendevousServerClient.registerTask(relayDst);


    final ContextManager manager = channel.pipeline().get(ContextManager.class);
    //LOG.info("Getting context manager!!!");
    completableFuture.complete(manager.newInputContext(srcExecutorId, descriptor, true));
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
          //LOG.info("Connected to relay server {}:{}, {}", address, port, connectFuture.channel());
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
