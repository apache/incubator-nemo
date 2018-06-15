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
package edu.snu.nemo.runtime.common;

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
