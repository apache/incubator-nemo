/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.nemo.runtime.executor.bytetransfer;

import edu.snu.nemo.conf.JobConf;
import edu.snu.nemo.runtime.common.NettyChannelImplementationSelector;
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
final class ByteTransport implements AutoCloseable {

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
      @Parameter(JobConf.PartitionTransportServerPort.class) final int port,
      @Parameter(JobConf.PartitionTransportServerBacklog.class) final int serverBacklog,
      @Parameter(JobConf.PartitionTransportServerNumListeningThreads.class) final int numListeningThreads,
      @Parameter(JobConf.PartitionTransportServerNumWorkingThreads.class) final int numWorkingThreads,
      @Parameter(JobConf.PartitionTransportClientNumThreads.class) final int numClientThreads) {

    this.nameResolver = nameResolver;

    if (port < 0) {
      throw new IllegalArgumentException(String.format("Invalid ByteTransportPort: %d", port));
    }

    final String host = localAddressProvider.getLocalAddress();

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

    try {
      final ByteTransportIdentifier identifier = new ByteTransportIdentifier(localExecutorId);
      nameResolver.register(identifier, (InetSocketAddress) listeningChannel.localAddress());
    } catch (final Exception e) {
      LOG.error("Cannot register ByteTransport listening address to the naming registry", e);
      throw new RuntimeException(e);
    }

    LOG.info("ByteTransport server in {} is listening at {}", localExecutorId, listeningChannel.localAddress());
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

  /**
   * Connect to the {@link ByteTransport} server of the specified executor.
   * @param remoteExecutorId  the id of the executor
   * @return a {@link ChannelFuture} for connecting
   */
  ChannelFuture connectTo(final String remoteExecutorId) {
    final InetSocketAddress address;
    try {
      final ByteTransportIdentifier identifier = new ByteTransportIdentifier(remoteExecutorId);
      address = nameResolver.lookup(identifier);
    } catch (final Exception e) {
      LOG.error(String.format("Cannot lookup ByteTransport listening address of %s", remoteExecutorId), e);
      throw new RuntimeException(e);
    }
    final ChannelFuture connectFuture = clientBootstrap.connect(address);
    connectFuture.addListener(future -> {
      if (future.isSuccess()) {
        // Succeed to connect
        LOG.debug("Connected to {}", remoteExecutorId);
        return;
      }
      // Failed to connect
      if (future.cause() == null) {
        LOG.error("Failed to connect to {}", remoteExecutorId);
      } else {
        LOG.error(String.format("Failed to connect to %s", remoteExecutorId), future.cause());
      }
    });
    return connectFuture;
  }

  /**
   * @return {@link ChannelGroup} for active connections between this executor and remote executor.
   */
  ChannelGroup getChannelGroup() {
    return channelGroup;
  }

  /**
   * {@link Identifier} for {@link ByteTransfer}.
   */
  private static final class ByteTransportIdentifier implements Identifier {

    private final String executorId;

    /**
     * Creates a {@link ByteTransportIdentifier}.
     *
     * @param executorId id of the {@link edu.snu.nemo.runtime.executor.Executor}
     */
    private ByteTransportIdentifier(final String executorId) {
      this.executorId = executorId;
    }

    @Override
    public String toString() {
      return "byte://" + executorId;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final ByteTransportIdentifier that = (ByteTransportIdentifier) o;
      return executorId.equals(that.executorId);
    }

    @Override
    public int hashCode() {
      return executorId.hashCode();
    }
  }
}
