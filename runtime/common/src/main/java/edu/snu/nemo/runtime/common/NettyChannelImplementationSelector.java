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
