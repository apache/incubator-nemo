package org.apache.nemo.runtime.master;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.TaskLoc;
import org.apache.nemo.common.TaskLocationMap;
import org.apache.nemo.common.exception.IllegalMessageException;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.common.Task;
import org.apache.nemo.runtime.master.lambda.LambdaTaskContainerEventHandler;
import org.apache.nemo.runtime.master.scheduler.ExecutorRegistry;
import org.apache.nemo.runtime.master.scheduler.PairStageTaskManager;
import org.apache.nemo.runtime.master.scheduler.PendingTaskCollectionPointer;
import org.apache.nemo.runtime.message.MessageContext;
import org.apache.nemo.runtime.message.MessageEnvironment;
import org.apache.nemo.runtime.message.MessageListener;
import org.apache.reef.tang.annotations.Parameter;
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

  private final String optPolicy;

  private PendingTaskCollectionPointer pendingTaskCollectionPointer;

  @Inject
  private TaskScheduledMapMaster(final ExecutorRegistry executorRegistry,
                                 final PendingTaskCollectionPointer pendingTaskCollectionPointer,
                                 final MessageEnvironment messageEnvironment,
                                 final PairStageTaskManager pairStageTaskManager,
                                 final LambdaTaskContainerEventHandler lambdaEventHandler,
                                 @Parameter(JobConf.OptimizationPolicy.class) final String optPolicy) {
    this.pendingTaskCollectionPointer = pendingTaskCollectionPointer;
    this.scheduledStageTasks = new ConcurrentHashMap<>();
    this.executorRelayServerInfoMap = new ConcurrentHashMap<>();
    this.executorAddressMap = new ConcurrentHashMap<>();
    this.executorRegistry = executorRegistry;
    this.lambdaEventHandler = lambdaEventHandler;
    this.pairStageTaskManager = pairStageTaskManager;
    this.optPolicy= optPolicy;
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

  public Future<String> deactivateAndStopTask(final String taskId,
                                                 final boolean lambdaAffinity) {
    final String executorId = taskExecutorIdMap.get(taskId);
    LOG.info("Deactivate task " + taskId + " to executor " + executorId);
    final ExecutorRepresenter representer = executorRegistry.getExecutorRepresentor(executorId);
    deactivateTaskLambdaAffinityMap.put(taskId, lambdaAffinity);
    representer.deactivateLambdaTask(taskId);
    return CompletableFuture.supplyAsync(() -> {
      synchronized (deactivateTaskLambdaAffinityMap) {
        while (deactivateTaskLambdaAffinityMap.containsKey(taskId) ||
          !isTaskScheduled(taskId)) {
          try {
            deactivateTaskLambdaAffinityMap.wait(20);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
      return taskId;
    });
  }

  public void stopDeactivatedTask(final String taskId) {
    if (!deactivateTaskLambdaAffinityMap.containsKey(taskId)) {
      throw new RuntimeException("Task is not deactivated,, but try to move from lambda to vm " + taskId);
    }

    synchronized (deactivateTaskLambdaAffinityMap) {
      final boolean lambdaAffinity = deactivateTaskLambdaAffinityMap.get(taskId);
      deactivateTaskLambdaAffinityMap.remove(taskId);
      stopTask(taskId, lambdaAffinity);
    }
  }

  private void getDescendant(final String taskId, final List<String> l) {
    taskIdTaskMap.get(taskId).getDownstreamTasks().forEach((edge, downtasks) -> {
      downtasks.forEach(downstream -> {
        if (taskIdTaskMap.get(downstream).getUpstreamTaskSet().size() == 1) {
          if (!l.contains(downstream)) {
            l.add(downstream);
            getDescendant(downstream, l);
          }
        }
      });
    });
  }

  public Future<String> stopTask(final String parent, final boolean lambdaAffinity) {

    final String executorId = taskExecutorIdMap.get(parent);

    final List<String> descendants = new LinkedList<>();
    descendants.add(parent);

    // getDescendant(parent, descendants);
    // LOG.info("O2O descendant of {}: {}", parent, descendants);

    for (int i = descendants.size() - 1; i >= 0; i--) {
      final String taskId = descendants.get(i);
      LOG.info("Send task " + taskId + " stop to executor " + executorId  + ", parent " + parent);

      prevTaskExecutorIdMap.put(taskId, executorId);
      taskToBeStopped.add(taskId);

      final ExecutorRepresenter representer = executorRegistry.getExecutorRepresentor(executorId);
      final Map<String, List<String>> stageTaskMap = scheduledStageTasks.get(representer);

      if (lambdaAffinity) {
        taskIdTaskMap.get(taskId).setProperty(ResourcePriorityProperty.of(ResourcePriorityProperty.LAMBDA));
      } else {
        taskIdTaskMap.get(taskId).setProperty(ResourcePriorityProperty.of(ResourcePriorityProperty.COMPUTE));
      }

      LOG.info("Send stop signal {} to task {}", taskId.equals(parent), taskId);
      representer.stopTask(taskId, taskId.equals(parent));

      synchronized (stageTaskMap) {
        final String stageId = RuntimeIdManager.getStageIdFromTaskId(taskId);
        final List<String> stageTasks = stageTaskMap.getOrDefault(stageId, new ArrayList<>());
        stageTasks.removeIf(task -> task.equals(taskId));
      }
    }

    return CompletableFuture.supplyAsync(() -> {
      while (!descendants.stream().allMatch(tid -> isTaskScheduled(tid))) {
        try {
          Thread.sleep(20);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      return parent;
    });
  }

  public boolean isTaskScheduled(final String taskId) {
    return !taskToBeStopped.contains(taskId) && taskExecutorIdMap.containsKey(taskId);
  }

  public boolean isAllTasksScheduledAtStartTime() {
    return taskIdTaskMap.keySet().stream()
      .allMatch(t -> taskExecutorIdMap.containsKey(t));
  }

  public Task removeTask(final String taskId) {
    taskExecutorIdMap.remove(taskId);
    final Task t = taskIdTaskMap.get(taskId);
    // lambdaTaskMap.remove(taskId);
    taskToBeStopped.remove(taskId);
    return t;
  }

  // NOt used
  @Deprecated
  public void keepOnceCurrentTaskExecutorIdMap() {
    synchronized (prevTaskExecutorIdMap) {
      if (copied) {
        return;
      }
      copied = true;
      prevTaskExecutorIdMap.clear();
      prevTaskExecutorIdMap.putAll(taskExecutorIdMap);
    }
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

  private void executingTask(final String executorId, final String taskId) {
    final ExecutorRepresenter representer = executorRegistry.getExecutorRepresentor(executorId);

    scheduledStageTasks.putIfAbsent(representer, new HashMap<>());
    LOG.info("Put task {} to executor {}", taskId, representer.getExecutorId());

    if (representer.getExecutorId() == null) {
      throw new RuntimeException("Executor Id null for putting task scheduled " + executorId + ", " + taskId);
    }

    representer.onTaskExecutionStarted(taskIdTaskMap.get(taskId));

    taskOriginalExecutorIdMap.putIfAbsent(taskId, representer.getExecutorId());
    taskExecutorIdMap.put(taskId, representer.getExecutorId());

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

    if (optPolicy.contains("R1R3")) {
      //  Redirect task if it is partial and transient and it is moved from VM to LAMBDA
      if ((taskIdTaskMap.get(taskId).isParitalCombine() && taskIdTaskMap.get(taskId).isTransientTask())
        && representer.getContainerType().equals(ResourcePriorityProperty.LAMBDA)) {
        final String vmTaskId = pairStageTaskManager.getPairTaskEdgeId(taskId).get(0).left();
        final String vmExecutorId = taskExecutorIdMap.get(vmTaskId);
        LOG.info("Redirection to partial and transient task from {} to {}", vmTaskId, taskId);
        final ExecutorRepresenter vmExecutor = executorRegistry.getExecutorRepresentor(vmExecutorId);
        representer.activateLambdaTask(taskId, vmTaskId, vmExecutor);
      }
    }
  }

  public void setExecutorAddressInfo(final String executorId,
                                                  final String address, final int port) {
    executorAddressMap.put(executorId, Pair.of(address, port));
  }

  public void setRelayServerInfo(final String executorId,
                                         final String address, final int port) {
    executorRelayServerInfoMap.put(executorId, Pair.of(address, port));
  }

  public Map<String, Pair<String, Integer>> getExecutorAddressMap() {
    return executorAddressMap;
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
