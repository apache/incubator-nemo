package org.apache.nemo.runtime.master;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class SerializedTaskMap {

  private final Map<String, byte[]> serializedTaskMap = new ConcurrentHashMap<>();

  @Inject
  private SerializedTaskMap() {
  }

  public void setSerializedTask(final String taskId, final byte[] bytes) {
    serializedTaskMap.put(taskId, bytes);
  }

  public byte[] getSerializedTask(final String taskId) {
    return serializedTaskMap.get(taskId);
  }
}
