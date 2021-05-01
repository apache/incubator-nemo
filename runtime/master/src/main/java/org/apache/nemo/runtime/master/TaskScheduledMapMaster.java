package org.apache.nemo.runtime.master;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.TaskLoc;
import org.apache.nemo.common.TaskLocationMap;
import org.apache.nemo.common.exception.IllegalMessageException;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.common.Task;
import org.apache.nemo.runtime.master.lambda.LambdaTaskContainerEventHandler;
import org.apache.nemo.runtime.master.scheduler.ExecutorRegistry;
import org.apache.nemo.runtime.master.scheduler.PairStageTaskManager;
import org.apache.nemo.runtime.message.MessageContext;
import org.apache.nemo.runtime.message.MessageEnvironment;
import org.apache.nemo.runtime.message.MessageListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.nemo.runtime.message.MessageEnvironment.ListenerType.EXECUTOR_MESSAGE_LISTENER_ID;
import static org.apache.nemo.runtime.message.MessageEnvironment.ListenerType.LAMBDA_OFFLOADING_REQUEST_ID;
import static org.apache.nemo.runtime.message.MessageEnvironment.ListenerType.TASK_SCHEDULE_MAP_LISTENER_ID;

public final class TaskScheduledMapMaster {
  private static final Logger LOG = LoggerFactory.getLogger(TaskScheduledMapMaster.class.getName());

  private final ConcurrentMap<ExecutorRepresenter,
    Map<String, List<String>>> scheduledStageTasks;

  private final Map<String, Pair<String, Integer>> executorRelayServerInfoMap;

  private final Map<String, Pair<String, Integer>> executorAddressMap;

  private final ExecutorRegistry executorRegistry;

  private final Map<String, String> taskExecutorIdMap = new ConcurrentHashMap<>();

  private Map<String, String> prevTaskExecutorIdMap = new ConcurrentHashMap<>();

  private final Map<String, Task> taskIdTaskMap = new ConcurrentHashMap<>();

  private final Map<String, String> taskOriginalExecutorIdMap = new ConcurrentHashMap<>();

  private final Map<String, Task> lambdaTaskMap = new ConcurrentHashMap<>();

  private final LambdaTaskContainerEventHandler lambdaEventHandler;

  private final PairStageTaskManager pairStageTaskManager;

  @Inject
  private TaskScheduledMapMaster(final ExecutorRegistry executorRegistry,
                                 final MessageEnvironment messageEnvironment,
                                 final PairStageTaskManager pairStageTaskManager,
                                 final LambdaTaskContainerEventHandler lambdaEventHandler) {
    this.scheduledStageTasks = new ConcurrentHashMap<>();
    this.executorRelayServerInfoMap = new ConcurrentHashMap<>();
    this.executorAddressMap = new ConcurrentHashMap<>();
    this.executorRegistry = executorRegistry;
    this.lambdaEventHandler = lambdaEventHandler;
    this.pairStageTaskManager = pairStageTaskManager;
    messageEnvironment.setupListener(TASK_SCHEDULE_MAP_LISTENER_ID,
      new TaskScheduleMapReceiver());
  }

  public Task getTask(final String taskId) {
    return taskIdTaskMap.get(taskId);
  }

  public Map<String, Task> getTaskIdTaskMap() {
    return taskIdTaskMap;
  }

  private boolean copied = false;

  private final List<String> taskToBeStopped = new LinkedList<>();

  public boolean isPartial(final String stageId) {
    return taskIdTaskMap.values()
      .stream().filter(task -> task.isParitalCombine() &&
        RuntimeIdManager.getStageIdFromTaskId(task.getTaskId()).equals(stageId)
      ).findFirst().isPresent();
  }

  private final Map<String, Boolean> deactivateTaskLambdaAffinityMap = new HashMap<>();

  public synchronized Future<String> deactivateAndStopTask(final String taskId,
                                                 final boolean lambdaAffinity) {
    final String executorId = taskExecutorIdMap.get(taskId);
    LOG.info("Deactivate task " + taskId + " to executor " + executorId);
    final ExecutorRepresenter representer = executorRegistry.getExecutorRepresentor(executorId);
    deactivateTaskLambdaAffinityMap.put(taskId, lambdaAffinity);
    representer.deactivateLambdaTask(taskId);
    return CompletableFuture.supplyAsync(() -> {
      synchronized (this) {
        while (deactivateTaskLambdaAffinityMap.containsKey(taskId) ||
          !isTaskScheduled(taskId)) {
          try {
            wait(20);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
      return taskId;
    });
  }

  public synchronized void stopDeactivatedTask(final String taskId) {
    if (!deactivateTaskLambdaAffinityMap.containsKey(taskId)) {
      throw new RuntimeException("Task is not deactivated,, but try to move from lambda to vm " + taskId);
    }

    final boolean lambdaAffinity = deactivateTaskLambdaAffinityMap.get(taskId);
    deactivateTaskLambdaAffinityMap.remove(taskId);
    stopTask(taskId, lambdaAffinity);
  }

  public synchronized Future<String> stopTask(final String taskId, final boolean lambdaAffinity) {

    final String executorId = taskExecutorIdMap.get(taskId);

    LOG.info("Send task " + taskId + " stop to executor " + executorId);

    prevTaskExecutorIdMap.put(taskId, executorId);

    taskToBeStopped.add(taskId);

    final ExecutorRepresenter representer = executorRegistry.getExecutorRepresentor(executorId);
    final Map<String, List<String>> stageTaskMap = scheduledStageTasks.get(representer);

    if (lambdaAffinity) {
      taskIdTaskMap.get(taskId).setProperty(ResourcePriorityProperty.of(ResourcePriorityProperty.LAMBDA));
    } else {
      taskIdTaskMap.get(taskId).setProperty(ResourcePriorityProperty.of(ResourcePriorityProperty.COMPUTE));
    }

    representer.stopTask(taskId);

    synchronized (stageTaskMap) {
      final String stageId = RuntimeIdManager.getStageIdFromTaskId(taskId);
      final List<String> stageTasks = stageTaskMap.getOrDefault(stageId, new ArrayList<>());
      stageTasks.removeIf(task -> task.equals(taskId));
    }

    return CompletableFuture.supplyAsync(() -> {
      synchronized (this) {
        while (!isTaskScheduled(taskId)) {
          try {
            wait(20);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
      return taskId;
    });
  }

  public boolean isTaskScheduled(final String taskId) {
    return !taskToBeStopped.contains(taskId) && taskExecutorIdMap.containsKey(taskId);
  }

  public synchronized Task removeTask(final String taskId) {
    taskExecutorIdMap.remove(taskId);
    final Task t = taskIdTaskMap.get(taskId);
    // lambdaTaskMap.remove(taskId);
    taskToBeStopped.remove(taskId);
    return t;
  }

  public synchronized void keepOnceCurrentTaskExecutorIdMap() {
    if (copied) {
      return;
    }
    copied = true;
    prevTaskExecutorIdMap.clear();
    prevTaskExecutorIdMap.putAll(taskExecutorIdMap);
  }

  public String getTaskOriginalExecutorId(final String taskId) {
    return taskOriginalExecutorIdMap.get(taskId);
  }

  public Map<String, String> getPrevTaskExecutorIdMap() {
    return prevTaskExecutorIdMap;
  }

  public void tasksToBeScheduled(final List<Task> tasks) {
    tasks.forEach(task -> {
      taskIdTaskMap.put(task.getTaskId(), task);

      task.getPropertyValue(ResourcePriorityProperty.class).ifPresent(val -> {
        if (val.equals(ResourcePriorityProperty.LAMBDA)) {
          lambdaTaskMap.put(task.getTaskId(), task);
        }
      });
    });
  }

  public boolean isAllLambdaTaskExecuting() {
    for (final String lambdaTaskId : lambdaTaskMap.keySet()) {
      if (!taskExecutorIdMap.containsKey(lambdaTaskId)) {
        return false;
      }
    }

    return true;
  }

  private synchronized void executingTask(final String executorId, final String taskId) {
    final ExecutorRepresenter representer = executorRegistry.getExecutorRepresentor(executorId);

    scheduledStageTasks.putIfAbsent(representer, new HashMap<>());
    LOG.info("Put task {} to executor {}", taskId, representer.getExecutorId());

    if (representer.getExecutorId() == null) {
      throw new RuntimeException("Executor Id null for putting task scheduled " + executorId + ", " + taskId);
    }

    representer.onTaskExecutionStarted(taskId);

    taskOriginalExecutorIdMap.putIfAbsent(taskId, representer.getExecutorId());
    synchronized (taskExecutorIdMap) {
      taskExecutorIdMap.put(taskId, representer.getExecutorId());
    }

    final Map<String, List<String>> stageTaskMap = scheduledStageTasks.get(representer);

    synchronized (stageTaskMap) {
      final String stageId = RuntimeIdManager.getStageIdFromTaskId(taskId);
      final List<String> stageTasks = stageTaskMap.getOrDefault(stageId, new ArrayList<>());
      stageTaskMap.put(stageId, stageTasks);

      stageTasks.add(taskId);
    }

    // broadcast
    executorRegistry.viewExecutors(executors -> {
      executors.forEach(executor -> {
        executor.sendControlMessage(ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(EXECUTOR_MESSAGE_LISTENER_ID.ordinal())
          .setType(ControlMessage.MessageType.TaskScheduled)
          .setRegisteredExecutor(taskId + "," + representer.getExecutorId())
          .build());
      });
    });

    // Redirect task if it is partial and transient and it is moved from VM to LAMBDA
    if ((taskIdTaskMap.get(taskId).isParitalCombine() && taskIdTaskMap.get(taskId).isTransientTask())
      && representer.getContainerType().equals(ResourcePriorityProperty.LAMBDA)) {
      final String vmTaskId =  pairStageTaskManager.getPairTaskEdgeId(taskId).left();
      final String vmExecutorId = taskExecutorIdMap.get(vmTaskId);
      LOG.info("Redirection to partial and transient task from {} to {}", vmTaskId, taskId);
      final ExecutorRepresenter vmExecutor = executorRegistry.getExecutorRepresentor(vmExecutorId);
      representer.activateLambdaTask(taskId, vmTaskId, vmExecutor);
    }
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

  public ConcurrentMap<ExecutorRepresenter, Map<String, List<String>>> getScheduledStageTasks() {
    return scheduledStageTasks;
  }

  public Map<String, List<String>> getScheduledStageTasks(final ExecutorRepresenter representer) {
    return scheduledStageTasks.get(representer);
  }

  /**
   * Handler for control messages received.
   */
  final class TaskScheduleMapReceiver implements MessageListener<ControlMessage.Message> {
    @Override
    public void onMessage(final ControlMessage.Message message) {
      switch (message.getType()) {
        case TaskExecuting: {
          final ControlMessage.TaskExecutingMessage m = message.getTaskExecutingMsg();
          LOG.info("Receive task executing message {} from {}", m.getTaskId(), m.getExecutorId());
          executingTask(m.getExecutorId(), m.getTaskId());
          if (isAllLambdaTaskExecuting()) {
            lambdaEventHandler.onAllLambdaTaskScheduled();
          }
          break;
        }
        default: {
          throw new RuntimeException("Unsupported message type");
        }
      }
    }

    @Override
    public void onMessageWithContext(final ControlMessage.Message message, final MessageContext messageContext) {
      switch (message.getType()) {
        case TaskScheduled: {
          final String requestedTaskId = message.getRegisteredExecutor();
          synchronized (taskExecutorIdMap) {
            final String executorId = taskExecutorIdMap.get(requestedTaskId);
            //  LOG.info("Send reply for location of {}, {} / {}",
            //   messageContext.getRequestId(),
            //   requestedTaskId, executorId);

            messageContext.reply(ControlMessage.Message.newBuilder()
              .setId(messageContext.getRequestId())
              .setListenerId(EXECUTOR_MESSAGE_LISTENER_ID.ordinal())
              .setType(ControlMessage.MessageType.TaskScheduled)
              .setRegisteredExecutor(requestedTaskId + "," + executorId)
              .build());
          }
          break;
        }

        case CurrentScheduledTask: {
          final Collection<String> c = taskExecutorIdMap
            .entrySet()
            .stream()
            .map(entry -> entry.getKey() + "," + entry.getValue())
            .collect(Collectors.toList());

          messageContext.reply(
            ControlMessage.Message.newBuilder()
              .setId(messageContext.getRequestId())
              .setListenerId(TASK_SCHEDULE_MAP_LISTENER_ID.ordinal())
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
