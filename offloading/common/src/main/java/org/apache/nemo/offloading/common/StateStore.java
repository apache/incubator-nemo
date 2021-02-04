package org.apache.nemo.offloading.common;


import java.io.InputStream;
import java.io.OutputStream;

public interface StateStore {

  InputStream getStateStream(final String taskId);
  byte[] getBytes(final String taskId);
  void put(final String taskId, byte[] bytes);
  boolean containsState(final String taskId);
  void close();
}
