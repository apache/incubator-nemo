package org.apache.nemo.runtime.executor;

import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.runtime.executor.common.ExecutorThread;
import org.apache.nemo.runtime.executor.common.TaskExecutor;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class TaskExecutorMapWrapper {

  private final ConcurrentMap<TaskExecutor, Boolean> taskExecutorMap;
  private final ConcurrentMap<String, List<TaskExecutor>> stageTaskMap;
  private final ConcurrentMap<String, TaskExecutor> taskIdExecutorMap;
  private final ConcurrentMap<TaskExecutor, ExecutorThread> taskExecutorThreadMap;

  @Inject
  private TaskExecutorMapWrapper() {
    this.taskExecutorMap = new ConcurrentHashMap<>();
    this.stageTaskMap = new ConcurrentHashMap<>();
    this.taskIdExecutorMap = new ConcurrentHashMap<>();
    this.taskExecutorThreadMap = new ConcurrentHashMap<>();
  }

  public void putTaskExecutor(final TaskExecutor taskExecutor,
                              ExecutorThread thread) {
    taskExecutorMap.put(taskExecutor, true);
    taskIdExecutorMap.put(taskExecutor.getId(), taskExecutor);
    taskExecutorThreadMap.put(taskExecutor, thread);

    final String stageId = RuntimeIdManager.getStageIdFromTaskId(taskExecutor.getId());
    stageTaskMap.putIfAbsent(stageId, new ArrayList<>());

    final List<TaskExecutor> tasks = stageTaskMap.get(stageId);

    synchronized (tasks) {
      tasks.add(taskExecutor);
    }
  }

  public TaskExecutor getTaskExecutor(final String taskId) {
    if (!taskIdExecutorMap.containsKey(taskId)) {
      throw new RuntimeException("No task executor " + taskId);
    }
    return taskIdExecutorMap.get(taskId);
  }

  public void removeTask(String taskId) {
    final TaskExecutor e = taskIdExecutorMap.remove(taskId);
    final ExecutorThread et = taskExecutorThreadMap.remove(e);
    et.deleteTask(e);

    taskExecutorMap.remove(e);

    stageTaskMap.values().forEach(l -> {
      synchronized (l) {
        l.remove(e);
      }
    });
  }

  public ConcurrentMap<TaskExecutor, Boolean> getTaskExecutorMap() {
    return taskExecutorMap;
  }
}
