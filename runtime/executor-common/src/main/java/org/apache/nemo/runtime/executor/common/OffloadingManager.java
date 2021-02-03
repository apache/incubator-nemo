package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.offloading.common.TaskHandlingEvent;

public interface OffloadingManager {
  void createWorker(int num);
  void offloading(String taskId);
  void deoffloading(String taskId);
  void offloadIntermediateData(String taskId, TaskHandlingEvent data);
  void offloadSourceData(final String taskId,
                         final String edgeId,
                         final Object data,
                         final Serializer serializer);
}
