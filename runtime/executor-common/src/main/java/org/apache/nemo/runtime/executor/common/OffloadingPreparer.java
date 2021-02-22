package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.common.Task;

public interface OffloadingPreparer {

  void prepare(String taskId, byte[] bytes);
}
