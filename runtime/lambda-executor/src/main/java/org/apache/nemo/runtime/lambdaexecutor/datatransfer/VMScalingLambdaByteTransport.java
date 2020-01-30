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
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import org.apache.nemo.common.Pair;
import org.apache.nemo.runtime.executor.common.ByteTransportIdentifier;
import org.apache.nemo.runtime.lambdaexecutor.NetworkUtils;
import org.apache.reef.io.network.naming.NameResolver;
import org.apache.reef.tang.Tang;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Map;

/**
 * Bootstraps the server and connects to other servers on demand.
 */
public final class VMScalingLambdaByteTransport implements ScalingByteTransport {//implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(LambdaByteTransport.class);
  private static final String CLIENT = "byte:client";

  private final EventLoopGroup clientGroup;
  private final Bootstrap clientBootstrap;
  private final Map<String, InetSocketAddress> executorAddressMap;
  private final ChannelGroup channelGroup;
  private final Channel relayServerChannel;
  private final Boolean isSf;

  private EventLoopGroup serverListeningGroup;
  private EventLoopGroup serverWorkingGroup;
  private int bindingPort;
  private Channel serverListeningChannel;
  private String publicAddress;
  private final NameResolver nameResolver;

  public VMScalingLambdaByteTransport(
    final String localExecutorId,
    final NativeChannelImplementationSelector channelImplSelector,
    final LambdaByteTransportChannelInitializer channelInitializer,
    final Map<String, InetSocketAddress> executorAddressMap,
    final ChannelGroup channelGroup,
    final String relayServerAddres,
    final int relayServerPort,
    final NameResolver nameResolver,
    final boolean isSf) {

    this.executorAddressMap = executorAddressMap;
    this.channelGroup = channelGroup;
    this.isSf = isSf;
    this.nameResolver = nameResolver;

    clientGroup = channelImplSelector.newEventLoopGroup(10, new DefaultThreadFactory(CLIENT));

    clientBootstrap = new Bootstrap()
      .group(clientGroup)
      .channel(channelImplSelector.getChannelClass())
      .handler(channelInitializer)
      .option(ChannelOption.SO_REUSEADDR, true);

    if (isSf) {
      final ChannelFuture channelFuture = connectToRelayServer(relayServerAddres, relayServerPort);
      this.relayServerChannel = channelFuture.channel();
    } else {
      // this is vm scaling worker!!
      serverListeningGroup = channelImplSelector.newEventLoopGroup(16,
        new DefaultThreadFactory("vm:scaling:listening"));
      serverWorkingGroup = channelImplSelector.newEventLoopGroup(16,
        new DefaultThreadFactory("vm:scaling:working"));
      // vm scaling worker
      final ServerBootstrap serverBootstrap = new ServerBootstrap()
        .group(serverListeningGroup, serverWorkingGroup)
        .channel(channelImplSelector.getServerChannelClass())
        .childHandler(channelInitializer)
        .option(ChannelOption.SO_BACKLOG, 128)
        .option(ChannelOption.SO_REUSEADDR, true);

      final String host;
      try {
        this.publicAddress = NetworkUtils.getPublicIP();
        host = NetworkUtils.getLocalHostLANAddress().getHostAddress();
      } catch (UnknownHostException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

      try {
        Channel listeningChannel = null;
        final TcpPortProvider tcpPortProvider = Tang.Factory.getTang().newInjector().getInstance(TcpPortProvider.class);
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


        serverListeningChannel = listeningChannel;

        final ByteTransportIdentifier identifier = new ByteTransportIdentifier(localExecutorId);
        nameResolver.register(identifier, new InetSocketAddress(publicAddress, bindingPort));

        LOG.info("public address: {}, port: {}, executorId: {}, registering to nameResolver", publicAddress, bindingPort, localExecutorId);
        //executorAddressMap.put(localExecutorId, new InetSocketAddress(publicAddress, bindingPort));

        LOG.info("ByteTransport server in {} is listening at {}", localExecutorId, listeningChannel.localAddress());
        this.relayServerChannel = null;

      } catch (final Exception e) {
        e.printStackTrace();
        // LOG.error("Cannot register ByteTransport listening address to the naming registry", e);
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public Channel getRelayServerChannel() {
    return relayServerChannel;
  }

  public void registerTask(final Pair<String, Integer> edgeIndex, final boolean in) {
    // todo
  }

  @Override
  public void close() {
    channelGroup.flush();
    final ChannelGroupFuture channelGroupCloseFuture = channelGroup.close();
    final Future clientGroupCloseFuture = clientGroup.shutdownGracefully();
    channelGroupCloseFuture.awaitUninterruptibly();
    clientGroupCloseFuture.awaitUninterruptibly();
  }

  @Override
  public ChannelFuture connectToRelayServer(final String address, final int port) {

    final InetSocketAddress socketAddress = new InetSocketAddress(address, port);
    final ChannelFuture connectFuture = clientBootstrap.connect(socketAddress);
    connectFuture.addListener(future -> {
      if (future.isSuccess()) {
        // Succeed to connect
        LOG.info("Connected to relay server {}:{}", address, port);
        return;
      }
      // Failed to connect (Not logging the cause here, which is not very useful)
      LOG.error("Failed to connect to relay server {}:{}", address, port);
    });
    return connectFuture;
  }

  private InetSocketAddress getAddress(final String remoteExecutorId) {

    if (isSf) {
      final InetSocketAddress address = executorAddressMap.get(remoteExecutorId);
      LOG.info("RemoteExecutorId {} Address {}", remoteExecutorId, address);
      return address;

    } else {
      InetSocketAddress address;
      while (true) {
        try {
          final ByteTransportIdentifier identifier = new ByteTransportIdentifier(remoteExecutorId);
          address = nameResolver.lookup(identifier);
          LOG.info("Address of {}: {}", remoteExecutorId, address);
          //executorAddressMap.put(remoteExecutorId, address);
          return address;
        } catch (final Exception e) {
          LOG.error(String.format("Cannot lookup ByteTransport listening address of %s", remoteExecutorId), e);
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e1) {
            e1.printStackTrace();
          }
        }
      }
    }
  }

  @Override
  public ChannelFuture connectTo(final String remoteExecutorId) {

    final InetSocketAddress address = getAddress(remoteExecutorId);
    LOG.info("RemoteExecutorId {} Address {}", remoteExecutorId, address);

    final ChannelFuture connectFuture = clientBootstrap.connect(address);
    connectFuture.addListener(future -> {
      if (future.isSuccess()) {
        // Succeed to connect
        LOG.info("Connected to remote {}", remoteExecutorId);
        return;
      }
      // Failed to connect (Not logging the cause here, which is not very useful)
      LOG.error("Failed to connect to {}", remoteExecutorId);
    });
    return connectFuture;
  }

  public ChannelGroup getChannelGroup() {
    return channelGroup;
  }
}
