package org.apache.nemo.runtime.executor.common;

import io.netty.channel.Channel;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class LambdaChannelMap {

  public final Map<String, Channel> lambdaChannelMap;

  @Inject
  private LambdaChannelMap() {
    this.lambdaChannelMap = new ConcurrentHashMap<>();
  }
}
