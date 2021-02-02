package org.apache.nemo.runtime.executor;


import org.apache.reef.tang.annotations.DefaultImplementation;

import java.io.InputStream;

@DefaultImplementation(NettyVMStateStore.class)
public interface NettyStateStore {

  int getPort();
  InputStream getStateStream(final String taskId);
  void put(final String taskId, byte[] bytes);
  boolean containsState(final String taskId);
}
