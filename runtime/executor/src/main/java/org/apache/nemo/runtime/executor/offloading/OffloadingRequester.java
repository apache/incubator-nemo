package org.apache.nemo.runtime.executor.offloading;

import io.netty.channel.Channel;

public interface OffloadingRequester {
  void start();

  void createChannelRequest();

  void destroy();

  void close();

  void destroyChannel(Channel channel);
}