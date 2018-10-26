package org.apache.nemo.runtime.common;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.concurrent.ThreadFactory;

/**
 * Selects appropriate {@link io.netty.channel.Channel} implementation.
 */
@DefaultImplementation(NativeChannelImplementationSelector.class)
public interface NettyChannelImplementationSelector {

  /**
   * Creates a new {@link EventLoopGroup}.
   * @param numThreads    the number of threads
   * @param threadFactory the {@link ThreadFactory}
   * @return a new {@link EventLoopGroup}
   */
  EventLoopGroup newEventLoopGroup(int numThreads, final ThreadFactory threadFactory);

  /**
   * @return class for server channel
   */
  Class<? extends ServerChannel> getServerChannelClass();

  /**
   * @return class for channel
   */
  Class<? extends Channel> getChannelClass();
}
