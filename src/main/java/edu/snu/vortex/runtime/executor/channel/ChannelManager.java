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

import edu.snu.vortex.compiler.ir.Element;

import java.util.ArrayList;
import java.util.List;

/**
 * ChannelManager manages a list of {@link OutputChannel}.
 * It uses {@link ChannelSelector} to select some of output channels to write a record.
 */
public final class ChannelManager {
  private final List<OutputChannel> channels;
  private final ChannelSelector channelSelector;

  public ChannelManager(final List<OutputChannel> channels,
                        final ChannelSelector channelSelector) {
    this.channels = channels;
    this.channelSelector = channelSelector;
  }

  public List<OutputChannel> selectChannels(final Element record) {
    final List<OutputChannel> selectedChannels = new ArrayList<>();
    final int[] channelIndexs = channelSelector.selectChannels(record);
    for (int index : channelIndexs) {
      selectedChannels.add(channels.get(index));
    }

    return selectedChannels;
  }
}
