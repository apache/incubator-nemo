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
package org.apache.nemo.runtime.message.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.nemo.common.NettyServerTransport;
import org.apache.nemo.offloading.common.Pair;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.message.*;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.*;

public final class NettyMasterEnvironment implements MessageEnvironment {
  private static final Logger LOG = LoggerFactory.getLogger(NettyMasterEnvironment.class.getName());

  private final String senderId;

  private final ConcurrentMap<ListenerType, MessageListener> listenerConcurrentMap;
  private final NettyServerTransport transport;

  private final ControlMessageHandler controlMessageHandler;

  private final Map<String, Channel> channelMap;
  private final ReplyFutureMap<ControlMessage.Message> replyFutureMap;

  private final ExecutorService executorService;

  @Inject
  private NettyMasterEnvironment(
    final TcpPortProvider tcpPortProvider,
    final IdentifierFactory idFactory,
    @Parameter(MessageParameters.SenderId.class) final String senderId) throws Exception {

    this.transport = new NettyServerTransport(
      tcpPortProvider,
      new NettyChannelInitializer(),
      new NioEventLoopGroup(5,
        new DefaultThreadFactory("NettyMessageEnvironment")),
      false);

    this.senderId = senderId;
    this.listenerConcurrentMap = new ConcurrentHashMap<>();

    this.executorService = Executors.newCachedThreadPool();

    this.channelMap = new ConcurrentHashMap<>();
    this.replyFutureMap = new ReplyFutureMap<>();
    this.controlMessageHandler =
      new ControlMessageHandler("master", channelMap, listenerConcurrentMap, replyFutureMap, executorService);


  }

  final class NettyChannelInitializer extends ChannelInitializer<SocketChannel> {

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
      ch.pipeline()
        .addLast("frameDecoder", new LengthFieldBasedFrameDecoder(
          Integer.MAX_VALUE, 0, 4, 0, 4))
        .addLast("frameEncoder", new LengthFieldPrepender(4))
        .addLast("decoder", new NettyControlMessageCodec.Decoder(false))
        .addLast("encoder", new NettyControlMessageCodec.Encoder(false))
        .addLast(controlMessageHandler);
    }
  }

  @Override
  public <T> void setupListener(final ListenerType listenerId, final MessageListener<T> listener) {
    if (listenerConcurrentMap.putIfAbsent(listenerId, listener) != null) {
      throw new RuntimeException("A listener for " + listenerId + " was already setup");
    }
  }

  @Override
  public void removeListener(final ListenerType listenerId) {
    listenerConcurrentMap.remove(listenerId);
  }

  @Override
  public <T> Future<MessageSender<T>> asyncConnect(final String receiverId, final ListenerType listenerId) {
    final Pair<String, ListenerType> key = Pair.of(receiverId, listenerId);

    if (channelMap.containsKey(receiverId)) {
      final CompletableFuture<MessageSender<T>> future = new CompletableFuture<>();
      future.complete(
        (MessageSender<T>)
          new NettyMessageSender("master", channelMap.get(receiverId), replyFutureMap)
      );
      return future;
    }

    return new Future<MessageSender<T>>() {
      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
      }

      @Override
      public boolean isCancelled() {
        return false;
      }

      @Override
      public boolean isDone() {
        return channelMap.containsKey(receiverId);
      }

      @Override
      public MessageSender<T> get() throws InterruptedException, ExecutionException {
        while (!channelMap.containsKey(receiverId)) {
          Thread.sleep(100);
        }

        return (MessageSender<T>)
          new NettyMessageSender("master", channelMap.get(receiverId), replyFutureMap);
      }

      @Override
      public MessageSender<T> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        throw new RuntimeException("Not supported");
      }
    };
  }

  @Override
  public <T> Future<MessageSender<T>> asyncConnect(String receiverId, ListenerType listenerId, InetSocketAddress addr) {
    return null;
  }

  @Override
  public int getPort() {
    return transport.getPort();
  }

  @Override
  public String getId() {
    return senderId;
  }

  @Override
  public void close() throws Exception {
    transport.close();
  }

}
