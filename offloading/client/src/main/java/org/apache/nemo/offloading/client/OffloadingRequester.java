package org.apache.nemo.offloading.client;

public interface OffloadingRequester {
  void start();

  void createChannelRequest();

  void destroy();

  void close();
}
