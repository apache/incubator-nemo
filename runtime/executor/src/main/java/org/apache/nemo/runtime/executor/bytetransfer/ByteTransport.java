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
package org.apache.nemo.runtime.executor.bytetransfer;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.offloading.client.NetworkUtils;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.executor.common.ByteTransportIdentifier;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.runtime.executor.datatransfer.NettyChannelImplementationSelector;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.reef.io.network.naming.NameResolver;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Bootstraps the server and connects to other servers on demand.
 */
public final class ByteTransport implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(ByteTransport.class);
  private static final String SERVER_LISTENING = "byte:server:listening";
  private static final String SERVER_WORKING = "byte:server:working";
  private static final String CLIENT = "byte:client";

  private final ChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
  private final NameResolver nameResolver;

  private final EventLoopGroup serverListeningGroup;
  private final EventLoopGroup serverWorkingGroup;
  private final EventLoopGroup clientGroup;
  private final Bootstrap clientBootstrap;
  private final Channel serverListeningChannel;
  private final String publicAddress;
  private int bindingPort;
  private final String localExecutorId;

  /**
   * Constructs a byte transport and starts listening.
   * @param nameResolver          provides naming registry
   * @param localExecutorId       the id of this executor
   * @param channelImplSelector   provides implementation for netty channel
   * @param channelInitializer    initializes channel pipeline
   * @param tcpPortProvider       provides an iterator of random tcp ports
   * @param localAddressProvider  provides the local address of the node to bind to
   * @param port                  the listening port; 0 means random assign using {@code tcpPortProvider}
   * @param serverBacklog         the maximum number of pending connections to the server
   * @param numListeningThreads   the number of listening threads of the server
   * @param numWorkingThreads     the number of working threads of the server
   * @param numClientThreads      the number of client threads
   */
  @Inject
  private ByteTransport(
      final NameResolver nameResolver,
      @Parameter(JobConf.ExecutorId.class) final String localExecutorId,
      final NettyChannelImplementationSelector channelImplSelector,
      final ByteTransportChannelInitializer channelInitializer,
      final TcpPortProvider tcpPortProvider,
      final LocalAddressProvider localAddressProvider,
      final PersistentConnectionToMasterMap persistentConnectionToMasterMap,
      @Parameter(EvalConf.Ec2.class) final boolean ec2,
      @Parameter(JobConf.PartitionTransportServerPort.class) final int port,
      @Parameter(JobConf.PartitionTransportServerBacklog.class) final int serverBacklog,
      @Parameter(JobConf.PartitionTransportServerNumListeningThreads.class) final int numListeningThreads,
      @Parameter(JobConf.PartitionTransportServerNumWorkingThreads.class) final int numWorkingThreads,
      @Parameter(JobConf.PartitionTransportClientNumThreads.class) final int numClientThreads) {

    this.localExecutorId = localExecutorId;
    this.nameResolver = nameResolver;

    if (port < 0) {
      throw new IllegalArgumentException(String.format("Invalid ByteTransportPort: %d", port));
    }

    final String host;
    try {
      this.publicAddress = NetworkUtils.getPublicIP();
      /*
      if (ec2) {
        this.publicAddress = NetworkUtils.getPublicIP();
      } else {
        this.publicAddress = NetworkUtils.getLocalHostLANAddress().getHostAddress();
      }
      */
      host = NetworkUtils.getLocalHostLANAddress().getHostAddress();
    } catch (UnknownHostException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    //final String host = localAddressProvider.getLocalAddress();

    serverListeningGroup = channelImplSelector.newEventLoopGroup(numListeningThreads,
        new DefaultThreadFactory(SERVER_LISTENING));
    serverWorkingGroup = channelImplSelector.newEventLoopGroup(numWorkingThreads,
        new DefaultThreadFactory(SERVER_WORKING));
    clientGroup = channelImplSelector.newEventLoopGroup(numClientThreads, new DefaultThreadFactory(CLIENT));

    clientBootstrap = new Bootstrap()
        .group(clientGroup)
        .channel(channelImplSelector.getChannelClass())
        .handler(channelInitializer)
        .option(ChannelOption.SO_REUSEADDR, true);

    final ServerBootstrap serverBootstrap = new ServerBootstrap()
        .group(serverListeningGroup, serverWorkingGroup)
        .channel(channelImplSelector.getServerChannelClass())
        .childHandler(channelInitializer)
        .option(ChannelOption.SO_BACKLOG, serverBacklog)
        .option(ChannelOption.SO_REUSEADDR, true);

    Channel listeningChannel = null;
    if (port == 0) {
      for (final int candidatePort : tcpPortProvider) {
        try {
          final ChannelFuture future = serverBootstrap.bind(host, candidatePort).await();
          if (future.cause() != null) {
            LOG.debug(String.format("Cannot bind to %s:%d", host, candidatePort), future.cause());
          } else if (!future.isSuccess()) {
            LOG.debug("Cannot bind to {}:{}", host, candidatePort);
          } else {
            listeningChannel = future.channel();
            bindingPort = candidatePort;
            break;
          }
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          LOG.debug(String.format("Interrupted while binding to %s:%d", host, candidatePort), e);
        }
      }
      if (listeningChannel == null) {
        serverListeningGroup.shutdownGracefully();
        serverWorkingGroup.shutdownGracefully();
        clientGroup.shutdownGracefully();
        LOG.error("Cannot bind to {} with tcpPortProvider", host);
        throw new RuntimeException(String.format("Cannot bind to %s with tcpPortProvider", host));
      }
    } else {
      try {
        bindingPort = port;
        final ChannelFuture future = serverBootstrap.bind(host, port).await();
        if (future.cause() != null) {
          throw future.cause();
        } else if (!future.isSuccess()) {
          throw new RuntimeException("Cannot bind");
        } else {
          listeningChannel = future.channel();
        }
      } catch (final Throwable e) {
        serverListeningGroup.shutdownGracefully();
        serverWorkingGroup.shutdownGracefully();
        clientGroup.shutdownGracefully();
        LOG.error(String.format("Cannot bind to %s:%d", host, port), e);
        throw new RuntimeException(String.format("Cannot bind to %s:%d", host, port), e);
      }
    }

    serverListeningChannel = listeningChannel;

    LOG.info("public address: {}, port: {}, executorId: {}", publicAddress, bindingPort, localExecutorId);

    persistentConnectionToMasterMap
      .getMessageSender(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID).send(
      ControlMessage.Message.newBuilder()
        .setId(RuntimeIdManager.generateMessageId())
        .setListenerId(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID)
        .setType(ControlMessage.MessageType.LocalExecutorAddressInfo)
        .setLocalExecutorAddressInfoMsg(ControlMessage.LocalExecutorAddressInfoMessage.newBuilder()
          .setExecutorId(localExecutorId)
          .setAddress(publicAddress)
          .setPort(bindingPort)
          .build())
      .build());


    try {
      final ByteTransportIdentifier identifier = new ByteTransportIdentifier(localExecutorId);
      nameResolver.register(identifier,new InetSocketAddress(publicAddress, bindingPort));
      //executorAddressMap.put(localExecutorId, new InetSocketAddress(publicAddress, bindingPort));

    } catch (final Exception e) {
      LOG.error("Cannot register ByteTransport listening address to the naming registry", e);
      throw new RuntimeException(e);
    }

    LOG.info("ByteTransport server in {} is listening at {}", localExecutorId, listeningChannel.localAddress());
  }

  public String getPublicAddress() {
    return publicAddress;
  }

  public int getBindingPort() {
    return bindingPort;
  }

  /**
   * Closes all channels and releases all resources.
   */
  @Override
  public void close() {
    LOG.info("Stopping listening at {} and closing", serverListeningChannel.localAddress());

    final ChannelFuture closeListeningChannelFuture = serverListeningChannel.close();
    final ChannelGroupFuture channelGroupCloseFuture = channelGroup.close();
    final Future serverListeningGroupCloseFuture = serverListeningGroup.shutdownGracefully();
    final Future serverWorkingGroupCloseFuture = serverWorkingGroup.shutdownGracefully();
    final Future clientGroupCloseFuture = clientGroup.shutdownGracefully();

    closeListeningChannelFuture.awaitUninterruptibly();
    channelGroupCloseFuture.awaitUninterruptibly();
    serverListeningGroupCloseFuture.awaitUninterruptibly();
    serverWorkingGroupCloseFuture.awaitUninterruptibly();
    clientGroupCloseFuture.awaitUninterruptibly();
  }

  public InetSocketAddress getAndPutInetAddress(final String remoteExecutorId) {
    final InetSocketAddress address;
    try {
      final ByteTransportIdentifier identifier = new ByteTransportIdentifier(remoteExecutorId);
      address = nameResolver.lookup(identifier);
      LOG.info("Address of {}: {}", remoteExecutorId, address);
      //executorAddressMap.put(remoteExecutorId, address);
      return address;
    } catch (final Exception e) {
      LOG.error(String.format("Cannot lookup ByteTransport listening address of %s", remoteExecutorId), e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Connect to the {@link ByteTransport} server of the specified executor.
   * @param remoteExecutorId  the id of the executor
   * @return a {@link ChannelFuture} for connecting
   */
  ChannelFuture connectTo(final String remoteExecutorId) {

    final InetSocketAddress address = getAndPutInetAddress(remoteExecutorId);
    final ChannelFuture connectFuture = clientBootstrap.connect(address);
    connectFuture.addListener(future -> {
      if (future.isSuccess()) {
        // Succeed to connect
        LOG.debug("Connected to {}", remoteExecutorId);
        return;
      }
      // Failed to connect (Not logging the cause here, which is not very useful)
      LOG.error("Failed to connect to {}", remoteExecutorId);
    });
    return connectFuture;
  }

  /**
   * @return {@link ChannelGroup} for active connections between this executor and remote executor.
   */
  ChannelGroup getChannelGroup() {
    return channelGroup;
  }

}
