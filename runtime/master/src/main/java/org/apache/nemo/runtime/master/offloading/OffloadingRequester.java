package org.apache.nemo.runtime.master.offloading;

import io.netty.channel.Channel;

public interface OffloadingRequester {
  void start();

  void createChannelRequest(String address, int port, int requestId, String executorId);

  void destroy();

  void close();

  void destroyChannel(Channel channel);
}
