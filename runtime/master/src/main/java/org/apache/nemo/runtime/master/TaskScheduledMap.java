package org.apache.nemo.runtime.master;

import org.apache.nemo.common.Pair;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.nemo.runtime.master.scheduler.ExecutorRegistry;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public final class TaskScheduledMap {

  private final ConcurrentMap<ExecutorRepresenter,
    Map<String, List<Task>>> scheduledStageTasks;

  private final Map<String, ExecutorRepresenter> executorIdRepresentorMap;

  private final Map<String, Pair<String, Integer>> executorRelayServerInfoMap;

  private final ExecutorRegistry executorRegistry;

  @Inject
  private TaskScheduledMap(final ExecutorRegistry executorRegistry) {
    this.scheduledStageTasks = new ConcurrentHashMap<>();
    this.executorIdRepresentorMap = new ConcurrentHashMap<>();
    this.executorRelayServerInfoMap = new ConcurrentHashMap<>();
    this.executorRegistry = executorRegistry;
  }

  public void addTask(final ExecutorRepresenter representer, final Task task) {
    scheduledStageTasks.putIfAbsent(representer, new HashMap<>());
    executorIdRepresentorMap.putIfAbsent(representer.getExecutorId(), representer);

    final Map<String, List<Task>> stageTaskMap = scheduledStageTasks.get(representer);

    synchronized (stageTaskMap) {
      final String stageId = RuntimeIdManager.getStageIdFromTaskId(task.getTaskId());
      final List<Task> stageTasks = stageTaskMap.getOrDefault(stageId, new ArrayList<>());
      stageTaskMap.put(stageId, stageTasks);

      stageTasks.add(task);
    }
  }

  public synchronized void setRelayServerInfo(final String executorId,
                                         final String address, final int port) {
    executorRelayServerInfoMap.put(executorId, Pair.of(address, port));
  }

  public synchronized boolean isAllRelayServerInfoReceived() {
    final AtomicBoolean b = new AtomicBoolean(false);
    executorRegistry.viewExecutors(c -> {
      b.set(c.size() == executorRelayServerInfoMap.size());
    });

    return b.get();
  }

  public synchronized Map<String, Pair<String, Integer>> getExecutorRelayServerInfoMap() {
    return executorRelayServerInfoMap;
  }


  public ConcurrentMap<ExecutorRepresenter, Map<String, List<Task>>> getScheduledStageTasks() {
    return scheduledStageTasks;
  }

  public Map<String, List<Task>> getScheduledStageTasks(final ExecutorRepresenter representer) {
    return scheduledStageTasks.get(representer);
  }

  public ExecutorRepresenter getExecutorRepresenter(final String executorId) {
    return executorIdRepresentorMap.get(executorId);
  }
}
