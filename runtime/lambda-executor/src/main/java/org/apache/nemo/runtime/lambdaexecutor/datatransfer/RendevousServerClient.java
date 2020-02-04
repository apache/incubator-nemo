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
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import org.apache.nemo.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Bootstraps the server and connects to other servers on demand.
 */
public final class RendevousServerClient extends SimpleChannelInboundHandler {

  private static final Logger LOG = LoggerFactory.getLogger(RendevousServerClient.class);
  private static final String CLIENT = "byte:client";

  private final EventLoopGroup clientGroup;
  private final Bootstrap clientBootstrap;

  private final Map<Channel, Set<String>> registerTaskMap;

  private final String myRendevousServerAddress;
  private final int myRendevousServerPort;

  private final Map<String, String> dstAddressMap = new ConcurrentHashMap<>();

  private final Channel channel;

  private final Set<String> registerTasks;

  private final ConcurrentMap<String, Long> taskInputWatermarkMap;
  private final ConcurrentMap<String, Long> taskInputWatermarkRequestTime;
  private final ConcurrentMap<String, Object> taskLockMap;

  public RendevousServerClient(final String myRendevousServerAddress,
                               final int myRendevousServerPort) {

    final RendevousClientChannelInitializer initializer =
      new RendevousClientChannelInitializer(this);

    final EventLoopGroup clientGroup = new NioEventLoopGroup(2, new DefaultThreadFactory("relayClient"));
    final Bootstrap clientBootstrap = new Bootstrap()
      .group(clientGroup)
      .channel(NioSocketChannel.class)
      .handler(initializer)
      .option(ChannelOption.SO_REUSEADDR, true);

    this.clientGroup = clientGroup;
    this.clientBootstrap = clientBootstrap;
    this.registerTaskMap = new ConcurrentHashMap<>();
    this.myRendevousServerAddress = myRendevousServerAddress;
    this.myRendevousServerPort = myRendevousServerPort;

    this.taskInputWatermarkMap = new ConcurrentHashMap<>();
    this.taskInputWatermarkRequestTime = new ConcurrentHashMap<>();
    this.taskLockMap = new ConcurrentHashMap<>();

    this.registerTasks = new HashSet<>();

    //final ChannelFuture channelFuture = connectToRelayServer(relayServerAddress, relayServerPort);
    //this.relayServerChannel = channelFuture.channel();
    this.channel =
      connectToRendevousServer(myRendevousServerAddress, myRendevousServerPort)
        .awaitUninterruptibly()
        .channel();
  }

  public Optional<Long> requestWatermark(final String taskId) {
    // REQUEST every 200 ms
    taskLockMap.putIfAbsent(taskId, new Object());
    final Object lock = taskLockMap.get(taskId);
    synchronized (lock) {
      final long currTime = System.currentTimeMillis();
      if (taskInputWatermarkRequestTime.getOrDefault(taskId, 0L) + 200 < currTime) {
        //LOG.info("Request watermark {}", stageId);
        channel.writeAndFlush(new WatermarkRequest(taskId));
        taskInputWatermarkRequestTime.put(taskId, currTime);
      }

      if (taskInputWatermarkMap.containsKey(taskId)) {
        return Optional.of(taskInputWatermarkMap.get(taskId));
      } else {
        return Optional.empty();
      }
    }
  }

  public void sendWatermark(final String taskId, final long watermark) {
    channel.writeAndFlush(new WatermarkSend(taskId, watermark));
  }

  public void registerWatermark(final String taskId, final long watermark) {
    final Object lock = taskLockMap.get(taskId);

    synchronized (lock) {
      if (taskInputWatermarkMap.getOrDefault(taskId, 0L) < watermark) {
        taskInputWatermarkMap.put(taskId, watermark);
      }
    }
  }

  private final Map<String, BlockingQueue<String>> taskExecutorIdResponseMap = new HashMap<>();

  public String requestExecutorId(final String taskId) {

    taskExecutorIdResponseMap.putIfAbsent(taskId, new LinkedBlockingQueue<>());
    channel.writeAndFlush(new TaskExecutorIdRequest(taskId));

    LOG.info("Request executor id of {}", taskId);
    final BlockingQueue<String> queue = taskExecutorIdResponseMap.get(taskId);
    try {
      final String executorId = queue.take();
      LOG.info("Request executor id of {} result {}", taskId, executorId);
      return executorId;
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public void registerExecutorIdResponse(final TaskExecutorIdResponse response) {
    final BlockingQueue<String> queue = taskExecutorIdResponseMap.get(response.taskId);
    queue.add(response.executorId);
  }

  public void registerResponse(final RendevousResponse response) {
    //LOG.info("Registering RedevousResponse {}/{}", response.dst, response.address);
    dstAddressMap.put(response.dst, response.address);
  }

  public String requestAddress(final String dst) {


    //LOG.info("Requesting address {} ", dst);

    if (dstAddressMap.containsKey(dst)) {
      //LOG.info("End of Requesting address {} ", dst);
      return dstAddressMap.get(dst);
    }

    channel.writeAndFlush(new RendevousRequest(dst));

    while (!dstAddressMap.containsKey(dst)) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    //LOG.info("End of Requesting address {} ", dst);
    return dstAddressMap.get(dst);
  }

  public void registerTask(final String dst) {

    synchronized (registerTasks) {
      if (!registerTasks.contains(dst)) {
        registerTasks.add(dst);
        //LOG.info("Registering address {}", dst);
        channel.writeAndFlush(new RendevousRegister(dst));
      }
    }
  }

  public void close() {
    final Future clientGroupCloseFuture = clientGroup.shutdownGracefully();
    clientGroupCloseFuture.awaitUninterruptibly();
  }

  public ChannelFuture connectToRendevousServer(final String address, final int port) {

    final Pair<String, Integer> key = Pair.of(address, port);

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
      throw new RuntimeException("Failed to connect to relay server ");
    });

    return connectFuture;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {

  }
}
