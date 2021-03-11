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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.offloading.common.Pair;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.message.*;
import org.apache.reef.io.network.naming.NameResolver;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.IdentifierFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.*;

public final class NettyWorkerEnvironment implements MessageEnvironment {
  private static final Logger LOG = LoggerFactory.getLogger(NettyWorkerEnvironment.class.getName());

  private static final String NCS_CONN_FACTORY_ID = "NCS_CONN_FACTORY_ID";

  private final String senderId;

  private final ConcurrentMap<ListenerType, MessageListener> listenerConcurrentMap;

  private final Bootstrap clientBootstrap;
  private final EventLoopGroup clientWorkerGroup;

  private final ControlMessageHandler controlMessageHandler;

  private final Map<String, Channel> channelMap;
  private final ReplyFutureMap<ControlMessage.Message> replyFutureMap;

  private final NameResolver nameResolver;
  private final IdentifierFactory identifierFactory;

  private final Map<String, Object> lockMap;

  private final ExecutorService executorService;

  @Inject
  private NettyWorkerEnvironment(
    final NameResolver nameResolver,
    final IdentifierFactory idFactory,
    @Parameter(MessageParameters.SenderId.class) final String senderId) {
    this.nameResolver = nameResolver;
    this.identifierFactory = idFactory;
    this.clientWorkerGroup = new NioEventLoopGroup(10,
      new DefaultThreadFactory("NettyWorkerEnvironment"));
    this.clientBootstrap = new Bootstrap();
    this.channelMap = new ConcurrentHashMap<>();
    this.clientBootstrap.group(clientWorkerGroup)
      .channel(NioSocketChannel.class)
      .handler(new NettyChannelInitializer())
      .option(ChannelOption.SO_REUSEADDR, true)
      .option(ChannelOption.SO_KEEPALIVE, true);

    this.senderId = senderId;
    this.lockMap = new ConcurrentHashMap<>();
    this.listenerConcurrentMap = new ConcurrentHashMap<>();
    this.replyFutureMap = new ReplyFutureMap<>();

    this.executorService = Executors.newFixedThreadPool(20);

    this.controlMessageHandler = new ControlMessageHandler(senderId,
      channelMap, listenerConcurrentMap, replyFutureMap, executorService);

  }

  final class NettyChannelInitializer extends ChannelInitializer<SocketChannel> {

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
      ch.pipeline()
        .addLast("frameDecoder", new LengthFieldBasedFrameDecoder(
          Integer.MAX_VALUE, 0, 4, 0, 4))
        .addLast("frameEncoder", new LengthFieldPrepender(4))
        .addLast("decoder", new NettyControlMessageCodec.Decoder(true))
        .addLast("encoder", new NettyControlMessageCodec.Encoder(true))
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
          new NettyMessageSender(senderId, channelMap.get(receiverId), replyFutureMap)
      );
      return future;
    } else {

      final Object obj = lockMap.putIfAbsent(receiverId, new Object());
      final ChannelFuture channelFuture;
      if (obj == null) {
        // Guarantee single connection for the same receiver
        final InetSocketAddress ipAddress;
        try {
          ipAddress = nameResolver.lookup(identifierFactory.getNewInstance(receiverId));
        } catch (Exception e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }

        channelFuture = clientBootstrap.connect(ipAddress);
      } else {
        channelFuture = null;
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
          if (channelFuture != null) {
            return channelFuture.isDone();
          } else {
            return channelMap.containsKey(receiverId);
          }
        }

        @Override
        public MessageSender<T> get() throws InterruptedException, ExecutionException {

          if (channelFuture != null) {
            channelFuture.awaitUninterruptibly();

            assert channelFuture.isDone();
            if (!channelFuture.isSuccess()) {
              final StringBuilder sb = new StringBuilder("A connection failed at Source - ");
              sb.append(channelFuture.cause());
              throw new RuntimeException(sb.toString());
            }

            final Channel channel = channelFuture.channel();

            LOG.info("Send init message from {}", senderId);

            // send InitRegistration message
            channel.writeAndFlush(new ControlMessageWrapper(ControlMessageWrapper.MsgType.Send,
              ControlMessage.Message.newBuilder()
                .setId(RuntimeIdManager.generateMessageId())
                .setListenerId(0) // not used
                .setSenderId(senderId)
                .setType(ControlMessage.MessageType.InitRegistration)
                .build()));

            channelMap.put(receiverId, channel);

          } else {
            while (!channelMap.containsKey(receiverId)) {
              Thread.sleep(100);
            }
          }

          final Channel channel = channelMap.get(receiverId);
          return (MessageSender<T>) new NettyMessageSender(senderId, channel, replyFutureMap);
        }

        @Override
        public MessageSender<T> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
          throw new RuntimeException("Not supported");
        }
      };
    }
  }

  @Override
  public String getId() {
    return senderId;
  }

  @Override
  public void close() throws Exception {
  }

}
