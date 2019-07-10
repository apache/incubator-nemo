package org.apache.nemo.runtime.executor.relayserver;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.offloading.client.NetworkUtils;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.runtime.executor.common.OutputWriterFlusher;
import org.apache.nemo.runtime.executor.datatransfer.NioChannelImplementationSelector;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class RelayServer {
  private static final Logger LOG = LoggerFactory.getLogger(RelayServer.class);

  private static final String SERVER_LISTENING = "relay:server:listening";
  private static final String SERVER_WORKING = "relay:server:working";

  private final ChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
  private final EventLoopGroup serverListeningGroup;
  private final Channel serverListeningChannel;
  private final EventLoopGroup serverWorkingGroup;

  private final String publicAddress;
  private int bindingPort;

  private final ConcurrentMap<String, Channel> channelMap = new ConcurrentHashMap<>();
  private final OutputWriterFlusher outputWriterFlusher;

  @Inject
  private RelayServer(@Parameter(JobConf.ExecutorId.class) final String executorId,
                      final TcpPortProvider tcpPortProvider,
                      @Parameter(EvalConf.Ec2.class) final boolean ec2,
                      final NioChannelImplementationSelector channelImplSelector) {

    final String host;
    try {
      if (ec2) {
        this.publicAddress = NetworkUtils.getPublicIP();
      } else {
        this.publicAddress = NetworkUtils.getLocalHostLANAddress().getHostAddress();
      }
      host = NetworkUtils.getLocalHostLANAddress().getHostAddress();
    } catch (UnknownHostException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    this.outputWriterFlusher = new OutputWriterFlusher(200);

    serverListeningGroup = channelImplSelector.newEventLoopGroup(2,
        new DefaultThreadFactory(SERVER_LISTENING));
    serverWorkingGroup = channelImplSelector.newEventLoopGroup(5,
        new DefaultThreadFactory(SERVER_WORKING));

    final ServerBootstrap serverBootstrap = new ServerBootstrap()
      .group(serverListeningGroup, serverWorkingGroup)
      .channel(channelImplSelector.getServerChannelClass())
      .childHandler(new RelayServerChannelInitializer(channelMap, outputWriterFlusher))
      .option(ChannelOption.SO_REUSEADDR, true);

    Channel listeningChannel = null;

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
      LOG.error("Cannot bind to {} with tcpPortProvider", host);
      throw new RuntimeException(String.format("Cannot bind to %s with tcpPortProvider", host));
    }

    serverListeningChannel = listeningChannel;

    LOG.info("Relay server public address: {}, port: {}", publicAddress, bindingPort);
  }

  public String getPublicAddress() {
    return publicAddress;
  }

  public int getPort() {
    return bindingPort;
  }
}
