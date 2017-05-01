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
package edu.snu.vortex.runtime.executor.channel_to_deprecate;

import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.runtime.exception.UnsupportedMethodException;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Local channel implementation.
 */
public final class LocalChannel implements OutputChannel, InputChannel {
  private final String channelId;
  private ChannelState channelState;
  private List<Element> recordList;

  public LocalChannel(final String channelId) {
    this.channelId = channelId;
    this.channelState = ChannelState.Close;
    this.recordList = new ArrayList<>();
  }

  public boolean isOpen() {
    return (channelState == ChannelState.Open);
  }

  @Nullable
  @Override
  public Iterable<Element> read() {
    if (!isOpen()) {
      return null;
    }

    List<Element> records = recordList;
    recordList = new ArrayList<>();
    return records;
  }

  @Override
  public Iterable<Element> read(final long timeout) {
    throw new UnsupportedMethodException("data read with timeout is not supported in" + this.getClass());
  }

  @Override
  public void write(final Iterable<Element> data) {
    if (isOpen()) {
      data.forEach(record -> recordList.add(record));
    }
  }

  @Override
  public void flush() {
    // No action
  }

  @Override
  public String getId() {
    return channelId;
  }

  @Override
  public ChannelState getState() {
    return channelState;
  }

  @Override
  public void initialize(final ChannelConfig config) {
    channelState = ChannelState.Open;
  }
}
