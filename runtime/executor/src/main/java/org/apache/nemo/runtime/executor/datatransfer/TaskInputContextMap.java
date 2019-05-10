package org.apache.nemo.runtime.executor.datatransfer;

import org.apache.nemo.runtime.executor.common.datatransfer.ByteInputContext;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class TaskInputContextMap {
  // key: (runtimeEdgeId, dstTaskIndex), value: input contexts of this task
  private final ConcurrentMap<String, List<ByteInputContext>> taskInputContextMap;

  @Inject
  private TaskInputContextMap() {
    this.taskInputContextMap = new ConcurrentHashMap<>();
  }

  public ConcurrentMap<String, List<ByteInputContext>> getTaskInputContextMap() {
    return taskInputContextMap;
  }

  public void put(final String taskId,
                  final ByteInputContext value) {
    taskInputContextMap.putIfAbsent(taskId, new ArrayList<>());
    taskInputContextMap.get(taskId).add(value);
  }
}
