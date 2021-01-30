package org.apache.nemo.common;


import java.io.InputStream;
import java.io.OutputStream;

public interface StateStore {

  InputStream getStateStream(final String taskId);
  OutputStream getOutputStreamForStoreTaskState(final String taskId);
  boolean containsState(final String taskId);
}
