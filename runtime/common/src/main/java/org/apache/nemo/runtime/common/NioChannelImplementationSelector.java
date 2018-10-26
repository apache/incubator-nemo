package org.apache.nemo.runtime.common;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import javax.inject.Inject;
import java.util.concurrent.ThreadFactory;

/**
 * A {@link NettyChannelImplementationSelector} implementation that prefers nio-based channel implementation.
 */
public final class NioChannelImplementationSelector implements NettyChannelImplementationSelector {

  /**
   * Private constructor.
   */
  @Inject
  private NioChannelImplementationSelector() {
  }

  @Override
  public EventLoopGroup newEventLoopGroup(final int numThreads, final ThreadFactory threadFactory) {
    return new NioEventLoopGroup(numThreads, threadFactory);
  }

  @Override
  public Class<? extends ServerChannel> getServerChannelClass() {
    return NioServerSocketChannel.class;
  }

  @Override
  public Class<? extends Channel> getChannelClass() {
    return NioSocketChannel.class;
  }
}
