package org.apache.nemo.runtime.common.offloading;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.nemo.common.NettyChannelInitializer;
import org.apache.nemo.runtime.common.NetworkUtils;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Iterator;

public final class NettyServerTransport {
  private static final Logger LOG = LoggerFactory.getLogger(NettyServerTransport.class.getName());
  private static final int SERVER_BOSS_NUM_THREADS = 3;
  private static final int SERVER_WORKER_NUM_THREADS = 10;
  private static final String CLASS_NAME = NettyServerTransport.class.getName();

  private EventLoopGroup serverBossGroup;
  private EventLoopGroup serverWorkerGroup;
  private Channel acceptor;

  private int port;
  private String localAddress;
  private final String publicAddress;

  public NettyServerTransport(final TcpPortProvider tcpPortProvider,
                              final ChannelInboundHandlerAdapter channelInboundHandlerAdapter) {
    this.serverBossGroup = new NioEventLoopGroup(SERVER_BOSS_NUM_THREADS,
      new DefaultThreadFactory(CLASS_NAME + "SourceServerBoss"));
    this.serverWorkerGroup = new NioEventLoopGroup(SERVER_WORKER_NUM_THREADS,
      new DefaultThreadFactory(CLASS_NAME + "SourceServerWorker"));

    final ServerBootstrap serverBootstrap = new ServerBootstrap();
    serverBootstrap.group(this.serverBossGroup, this.serverWorkerGroup)
      .channel(NioServerSocketChannel.class)
      .childHandler(new NettyChannelInitializer(channelInboundHandlerAdapter))
      .option(ChannelOption.SO_BACKLOG, 128)
      .option(ChannelOption.SO_REUSEADDR, true)
      .childOption(ChannelOption.SO_KEEPALIVE, true);

    this.publicAddress = NetworkUtils.getPublicIP();
    try {
      this.localAddress = NetworkUtils.getLocalHostLANAddress().getHostAddress();
    } catch (UnknownHostException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    this.port = setUpRandomPortNettyServer(serverBootstrap, tcpPortProvider);
    LOG.info("Public address: {}, localAddress: {}, port: {}", publicAddress, localAddress, port);
    LOG.info("Acceptor open: {}, active: {}", acceptor.isOpen(), acceptor.isActive());
  }

  public int getPort() {
    return port;
  }

  public String getPublicAddress() {
    return publicAddress;
  }

  public String getLocalAddress() {
    return localAddress;
  }

  private int setUpRandomPortNettyServer(final ServerBootstrap serverBootstrap,
                                         final TcpPortProvider tcpPortProvider) {
    try {
      final Iterator<Integer> portIterator = tcpPortProvider.iterator();
      while (true) {
        try {
          final int p = portIterator.next();
          this.acceptor = serverBootstrap.bind(
            new InetSocketAddress(localAddress, p)).sync().channel();
          LOG.info("Server address: {}, Assigned server port = {}", localAddress, p);
          return p;
        } catch (final Exception e) {
          e.printStackTrace();
          LOG.info("Server address: {}", localAddress);
          LOG.warn("Duplicate port is assigned to server... try again...");
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
