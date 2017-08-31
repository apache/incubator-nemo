/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.vortex.runtime.executor.data.partitiontransfer;

import edu.snu.vortex.client.JobConf;
import edu.snu.vortex.runtime.common.NettyChannelImplementationSelector;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import org.apache.reef.io.network.naming.NameResolver;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.net.InetSocketAddress;

/**
 * Bootstraps the server and connects to other servers on demand.
 */
final class PartitionTransport implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionTransport.class);
  private static final String SERVER_LISTENING = "partition:server:listening";
  private static final String SERVER_WORKING = "partition:server:working";
  private static final String CLIENT = "partition:client";

  private final InjectionFuture<PartitionTransfer> partitionTransfer;
  private final NameResolver nameResolver;

  private final EventLoopGroup serverListeningGroup;
  private final EventLoopGroup serverWorkingGroup;
  private final EventLoopGroup clientGroup;
  private final Bootstrap clientBootstrap;
  private final Channel serverListeningChannel;

  /**
   * Constructs a partition transport and starts listening.
   *
   * @param partitionTransfer     provides handler for inbound control messages
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
  private PartitionTransport(
      final InjectionFuture<PartitionTransfer> partitionTransfer,
      final NameResolver nameResolver,
      @Parameter(JobConf.ExecutorId.class) final String localExecutorId,
      final NettyChannelImplementationSelector channelImplSelector,
      final PartitionTransportChannelInitializer channelInitializer,
      final TcpPortProvider tcpPortProvider,
      final LocalAddressProvider localAddressProvider,
      @Parameter(JobConf.PartitionTransportServerPort.class) final int port,
      @Parameter(JobConf.PartitionTransportServerBacklog.class) final int serverBacklog,
      @Parameter(JobConf.PartitionTransportServerNumListeningThreads.class) final int numListeningThreads,
      @Parameter(JobConf.PartitionTransportServerNumWorkingThreads.class) final int numWorkingThreads,
      @Parameter(JobConf.PartitionTransportClientNumThreads.class) final int numClientThreads) {

    this.partitionTransfer = partitionTransfer;
    this.nameResolver = nameResolver;

    if (port < 0) {
      throw new IllegalArgumentException(String.format("Invalid PartitionTransportPort: %d", port));
    }

    final String host = localAddressProvider.getLocalAddress();

    serverListeningGroup = channelImplSelector.newEventLoopGroup(numListeningThreads,
        new DefaultThreadFactory(SERVER_LISTENING));
    serverWorkingGroup = channelImplSelector.newEventLoopGroup(numWorkingThreads,
        new DefaultThreadFactory(SERVER_WORKING));
    clientGroup = channelImplSelector.newEventLoopGroup(numClientThreads, new DefaultThreadFactory(CLIENT));

    clientBootstrap = new Bootstrap();
    clientBootstrap
        .group(clientGroup)
        .channel(channelImplSelector.getChannelClass())
        .handler(channelInitializer)
        .option(ChannelOption.SO_REUSEADDR, true);

    final ServerBootstrap serverBootstrap = new ServerBootstrap();
    serverBootstrap
        .group(serverListeningGroup, serverWorkingGroup)
        .channel(channelImplSelector.getServerChannelClass())
        .childHandler(channelInitializer)
        .option(ChannelOption.SO_BACKLOG, serverBacklog)
        .option(ChannelOption.SO_REUSEADDR, true);

    Channel listeningChannel = null;
    if (port == 0) {
      for (final int candidatePort : tcpPortProvider) {
        try {
          listeningChannel = serverBootstrap.bind(host, candidatePort).sync().channel();
          break;
        } catch (final InterruptedException e) {
          LOG.debug(String.format("Cannot bind to %s:%d", host, candidatePort), e);
        }
      }
      if (listeningChannel == null) {
        serverListeningGroup.shutdownGracefully();
        serverWorkingGroup.shutdownGracefully();
        clientGroup.shutdownGracefully();
        throw new RuntimeException(String.format("Cannot bind to %s with tcpPortProvider", host));
      }
    } else {
      try {
        listeningChannel = serverBootstrap.bind(host, port).sync().channel();
      } catch (final InterruptedException e) {
        serverListeningGroup.shutdownGracefully();
        serverWorkingGroup.shutdownGracefully();
        clientGroup.shutdownGracefully();
        throw new RuntimeException(String.format("Cannot bind to %s:%d", host, port), e);
      }
    }

    serverListeningChannel = listeningChannel;

    try {
      final PartitionTransportIdentifier identifier = new PartitionTransportIdentifier(localExecutorId);
      nameResolver.register(identifier, (InetSocketAddress) listeningChannel.localAddress());
    } catch (final Exception e) {
      LOG.error("Cannot register PartitionTransport listening address to the naming registry", e);
      throw new RuntimeException(e);
    }

    LOG.info("PartitionTransport server in {} is listening at {}", localExecutorId, listeningChannel.localAddress());
  }

  /**
   * Closes all channels and releases all resources.
   */
  @Override
  public void close() {
    LOG.info("Stopping listening at {} and closing", serverListeningChannel.localAddress());

    final ChannelFuture closeListeningChannelFuture = serverListeningChannel.close();
    final ChannelGroupFuture channelGroupCloseFuture = partitionTransfer.get().getChannelGroup().close();
    final Future serverListeningGroupCloseFuture = serverListeningGroup.shutdownGracefully();
    final Future serverWorkingGroupCloseFuture = serverWorkingGroup.shutdownGracefully();
    final Future clientGroupCloseFuture = clientGroup.shutdownGracefully();

    closeListeningChannelFuture.awaitUninterruptibly();
    channelGroupCloseFuture.awaitUninterruptibly();
    serverListeningGroupCloseFuture.awaitUninterruptibly();
    serverWorkingGroupCloseFuture.awaitUninterruptibly();
    clientGroupCloseFuture.awaitUninterruptibly();
  }

  /**
   * Connect to the {@link PartitionTransport} server of the specified executor.
   * @param remoteExecutorId the id of the executor
   * @return a {@link ChannelFuture} for connecting
   */
  ChannelFuture connectTo(final String remoteExecutorId) {
    final InetSocketAddress address;
    try {
      final PartitionTransportIdentifier identifier = new PartitionTransportIdentifier(remoteExecutorId);
      address = nameResolver.lookup(identifier);
    } catch (final Exception e) {
      LOG.error(String.format("Cannot lookup PartitionTransport listening address of %s", remoteExecutorId), e);
      throw new RuntimeException(e);
    }
    return clientBootstrap.connect(address);
  }

  /**
   * {@link Identifier} for {@link PartitionTransfer}.
   */
  private static final class PartitionTransportIdentifier implements Identifier {

    private final String executorId;

    /**
     * Creates a {@link PartitionTransportIdentifier}.
     *
     * @param executorId id of the {@link edu.snu.vortex.runtime.executor.Executor}
     */
    private PartitionTransportIdentifier(final String executorId) {
      this.executorId = executorId;
    }

    @Override
    public String toString() {
      return "partition://" + executorId;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final PartitionTransportIdentifier that = (PartitionTransportIdentifier) o;
      return executorId.equals(that.executorId);
    }

    @Override
    public int hashCode() {
      return executorId.hashCode();
    }
  }
}
