package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.runtime.executor.common.tasks.TaskExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class TaskExecutorMapWrapper {
  private static final Logger LOG = LoggerFactory.getLogger(TaskExecutorMapWrapper.class.getName());

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

    LOG.info("Put task {}", taskExecutor.getId());

    synchronized (tasks) {
      tasks.add(taskExecutor);
    }
  }

  // for testing
  public boolean containsTask(final String taskId) {
    return taskIdExecutorMap.containsKey(taskId);
  }

  public TaskExecutor getTaskExecutor(final String taskId) {
    if (!taskIdExecutorMap.containsKey(taskId)) {
      throw new RuntimeException("No task executor " + taskId);
    }
    return taskIdExecutorMap.get(taskId);
  }

  public synchronized void removeTask(String taskId) {
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

  public synchronized void forEach(EventHandler<TaskExecutor> handler) {
    taskExecutorMap.keySet().forEach(taskExecutor -> handler.onNext(taskExecutor));
  }

  public ExecutorThread getTaskExecutorThread(final String taskId) {
    return taskExecutorThreadMap.get(taskIdExecutorMap.get(taskId));
  }

  public ConcurrentMap<TaskExecutor, Boolean> getTaskExecutorMap() {
    return taskExecutorMap;
  }
}
