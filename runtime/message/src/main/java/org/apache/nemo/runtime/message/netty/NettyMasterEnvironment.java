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

import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.nemo.common.NettyServerTransport;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.message.*;
import org.apache.nemo.runtime.message.ncs.NcsMessageSender;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.Message;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.apache.reef.wake.remote.transport.LinkListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

public final class NettyMasterEnvironment implements MessageEnvironment {
  private static final Logger LOG = LoggerFactory.getLogger(NettyMasterEnvironment.class.getName());

  private static final String NCS_CONN_FACTORY_ID = "NCS_CONN_FACTORY_ID";

  private final IdentifierFactory idFactory;
  private final String senderId;

  private final ReplyFutureMap<ControlMessage.Message> replyFutureMap;
  private final ConcurrentMap<ListenerType, MessageListener> listenerConcurrentMap;
  private final Map<String, Connection> receiverToConnectionMap;

  private final NettyServerTransport transport;

  @Inject
  private NettyMasterEnvironment(
    final TcpPortProvider tcpPortProvider,
    final IdentifierFactory idFactory,
    @Parameter(MessageParameters.SenderId.class) final String senderId) {

    this.transport = new NettyServerTransport(
      tcpPortProvider,
      new NettyChannelInitializer(),
      new NioEventLoopGroup(5,
        new DefaultThreadFactory("NettyMessageEnvironment")),
      false);

    this.idFactory = idFactory;
    this.senderId = senderId;
    this.replyFutureMap = new ReplyFutureMap<>();
    this.listenerConcurrentMap = new ConcurrentHashMap<>();
    this.receiverToConnectionMap = new ConcurrentHashMap<>();
  }

  final class NettyChannelInitializer extends ChannelInitializer<SocketChannel> {

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
      ch.pipeline()
        .addLast("frameEncoder", new LengthFieldPrepender(4))
        .addLast("frameDecoder", new LengthFieldBasedFrameDecoder(
          Integer.MAX_VALUE, 0, 4, 0, 4));

        // TODO-1: todo

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
    /*
    try {
      // If the connection toward the receiver exists already, reuses it.
      final Connection connection;
      if (receiverToConnectionMap.containsKey(receiverId)) {
        connection = receiverToConnectionMap.get(receiverId);
      } else {
        connection = connectionFactory.newConnection(idFactory.getNewInstance(receiverId));
        connection.open();
      }
      return CompletableFuture.completedFuture((MessageSender) new NcsMessageSender(connection, replyFutureMap));
    } catch (final NetworkException e) {
      final CompletableFuture<MessageSender<T>> failedFuture = new CompletableFuture<>();
      failedFuture.completeExceptionally(e);
      return failedFuture;
    }
    */

    return null;
  }

  @Override
  public String getId() {
    return senderId;
  }

  @Override
  public void close() throws Exception {
  }
}
