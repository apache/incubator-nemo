package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.offloading.common.TaskHandlingEvent;

public interface OffloadingManager {
  // void prepareOffloading(String taskId, ExecutorThreadQueue et);
  void createWorker(int num);
  void offloading(String taskId);
  void deoffloading(String taskId);
  void offloadIntermediateData(String taskId, TaskHandlingEvent data);
  void offloadSourceData(final String taskId,
                         final String edgeId,
                         final Object data,
                         final Serializer serializer);
  void close();

  // For warm-up!!
  boolean offloadPartialDataOrNot(String taskId, TaskHandlingEvent data);
  boolean canOffloadPartial(String taskId);
  void invokeParitalOffloading();
}
