package org.apache.nemo.offloading.workers.vm;

public interface OffloadingRequester {
  void start();

  void createChannelRequest();

  void destroy();

  void close();
}
