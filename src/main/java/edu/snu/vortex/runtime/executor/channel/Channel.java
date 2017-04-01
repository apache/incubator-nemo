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
package edu.snu.vortex.runtime.executor.channel;


/**
 * Channel interface, a common part of {@link OutputChannel} and {@link InputChannel}.
 * Channel implementations should manage data transfers.
 */
public interface Channel {

  /**
   * @return the channel id.
   */
  String getId();

  /**
   * @return the state of this channel.
   */
  ChannelState getState();

  /**
   * Initialize the internal state of the channel.
   * @param config the configuration for channel initialization.
   */
  void initialize(ChannelConfig config);
}
