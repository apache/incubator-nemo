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
 * Uses {@link Epoll} if possible (on Linux).
 */
public final class NativeChannelImplementationSelector {

  /**
   * Private constructor.
   */
  public NativeChannelImplementationSelector() {
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

  public EventLoopGroup newEventLoopGroup(final int numThreads, final ThreadFactory threadFactory) {
    return EVENT_LOOP_GROUP_FUNCTION.apply(numThreads, threadFactory);
  }

  public Class<? extends ServerChannel> getServerChannelClass() {
    return SERVER_CHANNEL_CLASS;
  }

  public Class<? extends Channel> getChannelClass() {
    return CHANNEL_CLASS;
  }
}
