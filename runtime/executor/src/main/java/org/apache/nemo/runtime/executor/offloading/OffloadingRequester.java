package org.apache.nemo.runtime.executor.offloading;

public interface OffloadingRequester {
  void start();

  void createChannelRequest();

  void destroy();

  void close();
}
