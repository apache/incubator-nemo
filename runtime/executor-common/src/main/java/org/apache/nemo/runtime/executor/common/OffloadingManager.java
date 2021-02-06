package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.offloading.common.TaskHandlingEvent;

public interface OffloadingManager {
  // void prepareOffloading(String taskId, ExecutorThreadQueue et);
  void offloading(String taskId);
  void deoffloading(String taskId);
  void offloadIntermediateData(String taskId, TaskHandlingEvent data);
  void offloadSourceData(final String taskId,
                         final String edgeId,
                         final Object data,
                         final Serializer serializer);
  void close();
}
