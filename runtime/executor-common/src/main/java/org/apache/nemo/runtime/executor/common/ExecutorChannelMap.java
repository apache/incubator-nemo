package org.apache.nemo.runtime.executor.common;

import io.netty.channel.Channel;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class ExecutorChannelMap {

  public final Map<String, Channel> map;

  @Inject
  private ExecutorChannelMap() {
    this.map = new ConcurrentHashMap<>();
  }
}
