package org.apache.nemo.runtime.master;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.TaskLoc;
import org.apache.nemo.common.TaskLocationMap;
import org.apache.nemo.common.exception.IllegalMessageException;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageContext;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.MessageListener;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.nemo.runtime.master.scheduler.ExecutorRegistry;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public final class TaskScheduledMapMaster {

  private final ConcurrentMap<ExecutorRepresenter,
    Map<String, List<Task>>> scheduledStageTasks;

  private final Map<String, ExecutorRepresenter> executorIdRepresentorMap;

  private final Map<String, Pair<String, Integer>> executorRelayServerInfoMap;

  private final Map<String, Pair<String, Integer>> executorAddressMap;

  private final ExecutorRegistry executorRegistry;

  private final TaskLocationMap taskLocationMap;

  private final Map<String, Task> taskIdMap = new ConcurrentHashMap<>();

  private final Map<String, String> taskExecutorIdMap = new ConcurrentHashMap<>();

  private Map<String, String> prevTaskExecutorIdMap = new ConcurrentHashMap<>();

  @Inject
  private TaskScheduledMapMaster(final ExecutorRegistry executorRegistry,
                                 final MessageEnvironment messageEnvironment,
                                 final TaskLocationMap taskLocationMap) {
    this.scheduledStageTasks = new ConcurrentHashMap<>();
    this.executorIdRepresentorMap = new ConcurrentHashMap<>();
    this.executorRelayServerInfoMap = new ConcurrentHashMap<>();
    this.executorAddressMap = new ConcurrentHashMap<>();
    this.executorRegistry = executorRegistry;
    this.taskLocationMap = taskLocationMap;
    messageEnvironment.setupListener(MessageEnvironment.TASK_SCHEDULE_MAP_LISTENER_ID,
      new TaskScheduleMapReceiver());
  }

  private boolean copied = false;

  public Task removeTask(final String taskId) {
    final String executorId = taskExecutorIdMap.remove(taskId);
    prevTaskExecutorIdMap.remove(taskId);

    final ExecutorRepresenter representer = executorIdRepresentorMap.get(executorId);
    final Map<String, List<Task>> stageTaskMap = scheduledStageTasks.get(representer);

    synchronized (stageTaskMap) {
      final String stageId = RuntimeIdManager.getStageIdFromTaskId(taskId);
      final List<Task> stageTasks = stageTaskMap.getOrDefault(stageId, new ArrayList<>());
      stageTasks.removeIf(task -> task.getTaskId().equals(taskId));
    }

    return taskIdMap.remove(taskId);
  }

  public synchronized void keepOnceCurrentTaskExecutorIdMap() {
    if (copied) {
      return;
    }
    copied = true;
    prevTaskExecutorIdMap.clear();
    prevTaskExecutorIdMap.putAll(taskExecutorIdMap);
  }

  public Map<String, String> getPrevTaskExecutorIdMap() {
    return prevTaskExecutorIdMap;
  }

  public void addTask(final ExecutorRepresenter representer, final Task task) {

    taskIdMap.put(task.getTaskId(), task);
    scheduledStageTasks.putIfAbsent(representer, new HashMap<>());
    executorIdRepresentorMap.putIfAbsent(representer.getExecutorId(), representer);
    taskExecutorIdMap.put(task.getTaskId(), representer.getExecutorId());

    // Add task location to VM
    taskLocationMap.locationMap.put(task.getTaskId(), TaskLoc.VM);

    final Map<String, List<Task>> stageTaskMap = scheduledStageTasks.get(representer);

    synchronized (stageTaskMap) {
      final String stageId = RuntimeIdManager.getStageIdFromTaskId(task.getTaskId());
      final List<Task> stageTasks = stageTaskMap.getOrDefault(stageId, new ArrayList<>());
      stageTaskMap.put(stageId, stageTasks);

      stageTasks.add(task);
    }

    // broadcast
    executorRegistry.viewExecutors(executors -> {
      executors.forEach(executor -> {
        if (!executor.equals(representer)) {
          executor.sendControlMessage(ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(MessageEnvironment.TASK_SCHEDULE_MAP_LISTENER_ID)
          .setType(ControlMessage.MessageType.TaskScheduled)
          .setRegisteredExecutor(task.getTaskId() + "," + representer.getExecutorId())
          .build());
        }
      });
    });
  }

  public synchronized void setExecutorAddressInfo(final String executorId,
                                                  final String address, final int port) {
    executorAddressMap.put(executorId, Pair.of(address, port));
  }

  public synchronized boolean isAllExecutorAddressReceived() {
    final AtomicBoolean b = new AtomicBoolean(false);
    executorRegistry.viewExecutors(c -> {
      b.set(c.size() == executorAddressMap.size());
    });

    return b.get();
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

  public synchronized Map<String, Pair<String, Integer>> getExecutorAddressMap() {
    return executorAddressMap;
  }

  public synchronized Map<String, Pair<String, Integer>> getExecutorRelayServerInfoMap() {
    return executorRelayServerInfoMap;
  }


  public Map<String, String> getTaskExecutorIdMap() {
    return taskExecutorIdMap;
  }

  public ConcurrentMap<ExecutorRepresenter, Map<String, List<Task>>> getScheduledStageTasks() {
    return scheduledStageTasks;
  }

  public Map<String, Task> getTaskIdMap() {
    return taskIdMap;
  }

  public Map<String, List<Task>> getScheduledStageTasks(final ExecutorRepresenter representer) {
    return scheduledStageTasks.get(representer);
  }

  public ExecutorRepresenter getExecutorRepresenter(final String executorId) {
    return executorIdRepresentorMap.get(executorId);
  }



  /**
   * Handler for control messages received.
   */
  final class TaskScheduleMapReceiver implements MessageListener<ControlMessage.Message> {
    @Override
    public void onMessage(final ControlMessage.Message message) {
      throw new RuntimeException("Exception " + message);
    }

    @Override
    public void onMessageWithContext(final ControlMessage.Message message, final MessageContext messageContext) {
      switch (message.getType()) {
        case CurrentScheduledTask: {
          final Collection<String> c = taskExecutorIdMap
            .entrySet()
            .stream()
            .map(entry -> entry.getKey() + "," + entry.getValue())
            .collect(Collectors.toList());

          messageContext.reply(
            ControlMessage.Message.newBuilder()
              .setId(RuntimeIdManager.generateMessageId())
              .setListenerId(MessageEnvironment.TASK_SCHEDULE_MAP_LISTENER_ID)
              .setType(ControlMessage.MessageType.CurrentScheduledTask)
              .addAllCurrScheduledTasks(c)
              .build());
          break;
        }
        default:
          throw new IllegalMessageException(new Exception(message.toString()));
      }
    }
  }
}
