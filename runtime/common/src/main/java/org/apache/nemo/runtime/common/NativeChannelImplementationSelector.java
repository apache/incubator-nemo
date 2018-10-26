package org.apache.nemo.runtime.common;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import javax.inject.Inject;
import java.util.concurrent.ThreadFactory;
import java.util.function.BiFunction;

/**
 * A {@link NettyChannelImplementationSelector} implementation that prefers native transport if possible.
 * Uses {@link Epoll} if possible (on Linux).
 */
public final class NativeChannelImplementationSelector implements NettyChannelImplementationSelector {

  /**
   * Private constructor.
   */
  @Inject
  private NativeChannelImplementationSelector() {
  }

  // We may want to add selection of KQueue (for BSD). This requires higher version of netty.

  private static final BiFunction<Integer, ThreadFactory, EventLoopGroup> EVENT_LOOP_GROUP_FUNCTION =
      Epoll.isAvailable() ? (numThreads, threadFactory) -> new EpollEventLoopGroup(numThreads, threadFactory)
          : (numThreads, threadFactory) -> new NioEventLoopGroup(numThreads, threadFactory);
  private static final Class<? extends ServerChannel> SERVER_CHANNEL_CLASS =
      Epoll.isAvailable() ? EpollServerSocketChannel.class
          : NioServerSocketChannel.class;
  private static final Class<? extends Channel> CHANNEL_CLASS =
      Epoll.isAvailable() ? EpollSocketChannel.class
          : NioSocketChannel.class;

  @Override
  public EventLoopGroup newEventLoopGroup(final int numThreads, final ThreadFactory threadFactory) {
    return EVENT_LOOP_GROUP_FUNCTION.apply(numThreads, threadFactory);
  }

  @Override
  public Class<? extends ServerChannel> getServerChannelClass() {
    return SERVER_CHANNEL_CLASS;
  }

  @Override
  public Class<? extends Channel> getChannelClass() {
    return CHANNEL_CLASS;
  }
}
