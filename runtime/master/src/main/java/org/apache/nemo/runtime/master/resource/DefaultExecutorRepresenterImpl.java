/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.runtime.master.resource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourceSlotProperty;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.common.Task;
import org.apache.nemo.runtime.master.*;
import org.apache.nemo.runtime.master.scheduler.ExecutorRegistry;
import org.apache.nemo.runtime.master.scheduler.PairStageTaskManager;
import org.apache.nemo.runtime.message.MessageSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.nemo.runtime.message.MessageEnvironment.ListenerType.EXECUTOR_MESSAGE_LISTENER_ID;

/**
 * (WARNING) This class is not thread-safe, and thus should only be accessed through ExecutorRegistry.
 *
 * Contains information/state regarding an executor.
 * Such information may include:
 *    a) The executor's resource type.
 *    b) The executor's capacity (ex. number of cores).
 *    c) Tasks scheduled/launched for the executor.
 *    d) Name of the physical node which hosts this executor.
 *    e) (Please add other information as we implement more features).
 */
@NotThreadSafe
public final class DefaultExecutorRepresenterImpl implements ExecutorRepresenter {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultExecutorRepresenterImpl.class.getName());

  private final String executorId;
  private final ResourceSpecification resourceSpecification;
  private final Map<String, Task> runningComplyingTasks;
  private final Map<String, Task> runningNonComplyingTasks;
  private final Map<Task, Integer> scheduledTaskToAttempt;
  private final Set<String> tasksToBeStopped;
  private final Set<Task> completeTasks;
  private final Set<Task> failedTasks;
  private final MessageSender<ControlMessage.Message> messageSender;
  private final ExecutorShutdownHandler executorShutdownHandler;
  private final ExecutorService serializationExecutorService;
  private final String nodeName;
  private final SerializedTaskMap serializedTaskMap;
  private final Set<Task> runningTasks;

  private WorkerControlProxy lambdaControlProxy;

  private final ScheduledExecutorService scheduledService;

  /**
   * Creates a reference to the specified executor.
   * @param executorId the executor id
   * @param resourceSpecification specification for the executor
   * @param messageSender provides communication context for this executor
   * @param serializationExecutorService provides threads for message serialization
   * @param nodeName physical name of the node where this executor resides
   */
  public DefaultExecutorRepresenterImpl(final String executorId,
                                        final ResourceSpecification resourceSpecification,
                                        final MessageSender<ControlMessage.Message> messageSender,
                                        final ExecutorShutdownHandler executorShutdownHandler,
                                        final ExecutorService serializationExecutorService,
                                        final String nodeName,
                                        final SerializedTaskMap serializedTaskMap) {
    this.executorId = executorId;
    this.resourceSpecification = resourceSpecification;
    this.messageSender = messageSender;
    this.runningComplyingTasks = new ConcurrentHashMap<>();
    this.runningNonComplyingTasks = new ConcurrentHashMap<>();
    this.scheduledTaskToAttempt = new ConcurrentHashMap<>();
    this.tasksToBeStopped = new HashSet<>();
    this.completeTasks = new HashSet<>();
    this.failedTasks = new HashSet<>();
    this.runningTasks = new HashSet<>();
    this.executorShutdownHandler = executorShutdownHandler;
    this.serializationExecutorService = serializationExecutorService;
    this.nodeName = nodeName;
    this.serializedTaskMap = serializedTaskMap;
    this.scheduledService = Executors.newScheduledThreadPool(10);
  }

  @Override
  public void setLambdaControlProxy(final WorkerControlProxy workerControlProxy) {
    this.lambdaControlProxy = workerControlProxy;
  }

  @Override
  public WorkerControlProxy getLambdaControlProxy() {
    return lambdaControlProxy;
  }

  private final Set<String> activatedTasks = new HashSet<>();
  private final Set<String> activatedPendingTasks = new HashSet<>();

  private void waitForActivation() {
    while (!lambdaControlProxy.isActive()) {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  private void sendRoutingSignal(final String taskId, final String pairVmTaskId, ExecutorRepresenter vmExecutor) {
    // waiting
    waitForActivation();

    activatedPendingTasks.add(taskId);

    // send signal
    LOG.info("Activation lambda task {} / pairTask {} for executor {}, {}", taskId,
      pairVmTaskId, executorId, activatedPendingTasks);
    vmExecutor.sendControlMessage(ControlMessage.Message.newBuilder()
      .setId(RuntimeIdManager.generateMessageId())
      .setListenerId(EXECUTOR_MESSAGE_LISTENER_ID.ordinal())
      .setType(ControlMessage.MessageType.RoutingDataToLambda)
      .setStopTaskMsg(ControlMessage.StopTaskMessage.newBuilder()
        .setTaskId(pairVmTaskId)
        .build())
      .build());
  }

  private boolean partialWarmupStarted = false;

  @Override
  public synchronized void partialWarmupStatelessTasks(final double percent,
                                                       final TaskScheduledMapMaster taskScheduledMapMaster,
                                                       final ExecutorRegistry executorRegistry,
                                                       final PairStageTaskManager pairStageTaskManager) {

    if (!lambdaControlProxy.isDeactivated()) {
      LOG.info("Skip partial warmup because lambda is active");
      return;
    }

    if (!activatedTasks.isEmpty() || !activatedPendingTasks.isEmpty()) {
      LOG.info("Skip partial warmup because lambda has active tasks {} / {}",
        activatedTasks, activatedPendingTasks);
      return;
    }


    final Collection<String> statelessTasks = getRunningTasks().stream()
      .filter(task -> !task.isStateful())
      .map(task -> task.getTaskId())
      .sorted((t1, t2) -> Integer.valueOf(t1.split("-")[0].split("Stage")[1]).compareTo(
        Integer.valueOf(t2.split("-")[0].split("Stage")[1])))
      .collect(Collectors.toSet());

    final Set<String> tasks = statelessTasks
      .stream()
      .limit((int)(percent * statelessTasks.size())).collect(Collectors.toSet());

    tasks.forEach(tid -> {
      final String pairTid = pairStageTaskManager.getPairTaskEdgeId(tid).left();
      final ExecutorRepresenter vm = executorRegistry.getExecutorRepresentor(
        taskScheduledMapMaster.getTaskExecutorIdMap().get(pairTid));
      activateLambdaTask(tid, pairTid, vm);
    });


    LOG.info("Waiting for partial warmup activation {}/{}", executorId, tasks);
    partialWarmupStarted = true;
    while (tasks.stream().anyMatch(tid -> !activatedTasks.contains(tid))) {
      try {
        wait(30);
      } catch (InterruptedException e) {
        LOG.info("Interrupted 11..");
      }
    }

    LOG.info("End of waiting for partial warmup activation {}/{}", executorId, tasks);
    try {
      Thread.sleep(800);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    LOG.info("Deactivate partial warmup");

    tasks.forEach(tid -> {
      deactivateLambdaTask(tid);
    });

    LOG.info("Waiting for deactivation of {} after partial warmup", executorId);
    while (!lambdaControlProxy.isDeactivated()) {
      try {
        wait(30);
      } catch (InterruptedException e) {
      }
    }

    partialWarmupStarted = false;
    LOG.info("End of waiting for deactivation of {} after partial warmup", executorId);
  }

  @Override
  public synchronized void onTaskExecutionStarted(final String taksId) {
    runningTasks.add(runningComplyingTasks.get(taksId));
  }

  @Override
  public synchronized void activateLambdaTask(final String taskId, final String pairVmTaskId, ExecutorRepresenter vmExecutor) {
    if (lambdaControlProxy == null) {
      throw new RuntimeException("Lambda control proxy null " + executorId);
    }

    if (getRunningTasks().stream().noneMatch(task -> task.getTaskId().equals(taskId))) {
      throw new RuntimeException("Lambda executor " + executorId + " does not contain lambda task " + taskId);
    }

    while (partialWarmupStarted) {
      // wait for partial warmup
      try {
        wait(30);
      } catch (InterruptedException e) {
      }
    }

    if (lambdaControlProxy.isActive() || lambdaControlProxy.isActivating()) {
      sendRoutingSignal(taskId, pairVmTaskId, vmExecutor);
    } else {
      // activate signal
      lambdaControlProxy.activate();
      sendRoutingSignal(taskId, pairVmTaskId, vmExecutor);
    }
  }

  @Override
  public synchronized boolean isAllTaskActivated() {
    return lambdaControlProxy.isActive() && activatedPendingTasks.isEmpty();
  }

  private Task findTask(final String taskId) {
    for (final Task t : scheduledTaskToAttempt.keySet()) {
      if (t.getTaskId().equals(taskId)) {
        return t;
      }
    }
    return null;
  }

  @Override
  public synchronized boolean isAllTaskActivatedExceptPartial() {
    return lambdaControlProxy.isActive() && activatedPendingTasks
      .stream().noneMatch(taskId -> {
        final Task task = findTask(taskId);
        if (task == null) {
          throw new RuntimeException("Task is null " + taskId);
        }
        return !task.isParitalCombine();
      });
  }

  @Override
  public synchronized void activationDoneSignal(final String taskId) {
    if (!executorId.contains("Lambda")) {
      throw new RuntimeException("Non lambda executor receive signal " + executorId);
    }

    if (!activatedPendingTasks.contains(taskId)) {
      throw new RuntimeException("Task is not activated but receive activation done " + taskId + " in " + executorId
        + ", pending: " + activatedPendingTasks + ", active: " + activatedTasks);
    }

    LOG.info("Activation done lambda task {}", taskId);
    activatedPendingTasks.remove(taskId);
    activatedTasks.add(taskId);
  }

  @Override
  public synchronized void deactivationDoneSignal(final String taskId) {
    if (!activatedTasks.contains(taskId)) {
      throw new RuntimeException("Task is not activated but deactivate done signal " + taskId + " in " + executorId
        + ", activatedTasks " + activatedTasks);
    }
    activatedTasks.remove(taskId);
    LOG.info("Deactivation done of lambda task {} in {} / {}", taskId, executorId, activatedTasks);

    // Move lambda task
    checkAndDeactivate();
  }

  private void checkAndDeactivate() {
    if (activatedTasks.isEmpty() &&
      getScheduledTasks().isEmpty() &&
      getRunningTasks().isEmpty() &&
      lambdaControlProxy.isActive()) {
      LOG.info("Deactivate lambda worker {}", executorId);
      lambdaControlProxy.deactivate();
    }

    //  getScheduledTasks().stream().allMatch(task -> task.isParitalCombine())) {
    // If all partial task is deactivated
    // and other tasks are not scheduled in the lambda executor,
    // then deactivation of the worker
    //   }
  }

  @Override
  public synchronized void deactivateLambdaTask(final String taskId) {
    if (lambdaControlProxy == null) {
      throw new RuntimeException("Lambda control proxy null " + executorId);
    }
    if (getRunningTasks().stream().noneMatch(task -> task.getTaskId().equals(taskId))) {
      throw new RuntimeException("Lambda executor " + executorId + " does not contain lambda task " + taskId);
    }
    if (!activatedTasks.contains(taskId) && !activatedPendingTasks.contains(taskId)) {
      throw new RuntimeException("Lambda executor " + executorId + " does not contain activate task " + taskId);
    }
    if (!lambdaControlProxy.isActive()) {
      throw new RuntimeException("Lambda proxy " + executorId + " is not activated, but try to deactivate task " + taskId);
    }

    while (true) {
      if (!activatedPendingTasks.contains(taskId)) {
        LOG.info("Deactivation lambda task {} for executor {}", taskId, executorId);
        sendControlMessage(ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(EXECUTOR_MESSAGE_LISTENER_ID.ordinal())
          .setType(ControlMessage.MessageType.RoutingDataToLambda)
          .setStopTaskMsg(ControlMessage.StopTaskMessage.newBuilder()
            .setTaskId(taskId)
            .build())
          .build());

        return;
      }

      try {
        wait(40);
      } catch (InterruptedException e) {
      }
    }
  }

  /**
   * Marks all Tasks which were running in this executor as failed.
   *
   * @return set of identifiers of tasks that were running in this executor.
   */
  @Override
  public synchronized Set<String> onExecutorFailed() {
    failedTasks.addAll(runningComplyingTasks.values());
    failedTasks.addAll(runningNonComplyingTasks.values());
    final Set<String> taskIds = Stream.concat(runningComplyingTasks.keySet().stream(),
        runningNonComplyingTasks.keySet().stream()).collect(Collectors.toSet());
    runningComplyingTasks.clear();
    runningNonComplyingTasks.clear();
    return taskIds;
  }

  private Set<Task> prevScheduledTasks = new HashSet<>();

  /**
   * Marks the Task as running, and sends scheduling message to the executor.
   * @param task the task to run
   */
  @Override
  public synchronized void onTaskScheduled(final Task task) {
    (task.getPropertyValue(ResourceSlotProperty.class).orElse(true)
        ? runningComplyingTasks : runningNonComplyingTasks).put(task.getTaskId(), task);
    scheduledTaskToAttempt.put(task, task.getAttemptIdx());
    failedTasks.remove(task);

    if (task.isTransientTask()) {
      activatedTasks.add(task.getTaskId());
    }

    if (lambdaControlProxy != null) {
      if (lambdaControlProxy.isActive() || lambdaControlProxy.isActivating()) {
        // just send task
      } else {
        // activate signal
        LOG.info("Activate lambda before scheduling tasks");
        lambdaControlProxy.activate();
        waitForActivation();
      }
    }

    serializationExecutorService.execute(() -> {
      final ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
      final DataOutputStream dos = new DataOutputStream(bos);
      task.encode(dos);
      try {
        dos.close();
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

      serializedTaskMap.setSerializedTask(task.getTaskId(), bos.toByteArray());

      if (prevScheduledTasks.contains(task)) {
        sendControlMessage(
          ControlMessage.Message.newBuilder()
            .setId(RuntimeIdManager.generateMessageId())
            .setListenerId(EXECUTOR_MESSAGE_LISTENER_ID.ordinal())
            .setType(ControlMessage.MessageType.ScheduleCachedTask)
            .setStopTaskMsg(ControlMessage.StopTaskMessage
              .newBuilder()
              .setTaskId(task.getTaskId())
              .build())
            .build());
      } else {
        prevScheduledTasks.add(task);

        sendControlMessage(
          ControlMessage.Message.newBuilder()
            .setId(RuntimeIdManager.generateMessageId())
            .setListenerId(EXECUTOR_MESSAGE_LISTENER_ID.ordinal())
            .setType(ControlMessage.MessageType.ScheduleTask)
            .setScheduleTaskMsg(
              ControlMessage.ScheduleTaskMsg.newBuilder()
                .setTask(ByteString.copyFrom(bos.toByteArray()))
                .build())
            .build());
      }
    });
  }

  @Override
  public synchronized void stopTask(final String taskId) {
    scheduledTaskToAttempt.remove(runningComplyingTasks.get(taskId));
    runningTasks.remove(runningComplyingTasks.get(taskId));

    tasksToBeStopped.add(taskId);

    sendControlMessage(ControlMessage.Message.newBuilder()
      .setId(RuntimeIdManager.generateMessageId())
      .setListenerId(EXECUTOR_MESSAGE_LISTENER_ID.ordinal())
      .setType(ControlMessage.MessageType.StopTask)
      .setStopTaskMsg(ControlMessage.StopTaskMessage.newBuilder()
        .setTaskId(taskId)
        .build())
      .build());
  }

  /**
   * Sends control message to the executor.
   * @param message Message object to send
   */
  @Override
  public void sendControlMessage(final ControlMessage.Message message) {
    messageSender.send(message);
  }

  /**
   * Marks the specified Task as completed.
   * @param taskId id of the completed task
   */
  @Override
  public synchronized void onTaskExecutionComplete(final String taskId) {
    final Task completedTask = removeFromRunningTasks(taskId);
    completeTasks.add(completedTask);
  }

  /**
   * Marks the specified Task as completed.
   * @param taskId id of the completed task
   */
  @Override
  public synchronized void onTaskExecutionStop(final String taskId) {
    LOG.info("Intro: onTaskExecutionStop: {}", taskId);
    final Task completedTask = removeFromRunningTasks(taskId);
    if (!tasksToBeStopped.remove(taskId)) {
      throw new RuntimeException("Task " + taskId + " is not stopped, but got stop message");
    }

    if (lambdaControlProxy != null) {
      // this is lambda executor
      checkAndDeactivate();
    }
    LOG.info("Outro: onTaskExecutionStop: {}", taskId);
  }

  /**
   * Marks the specified Task as failed.
   * @param taskId id of the Task
   */
  @Override
  public synchronized void onTaskExecutionFailed(final String taskId) {
    final Task failedTask = removeFromRunningTasks(taskId);
    failedTasks.add(failedTask);
  }

  /**
   * @return how many Tasks can this executor simultaneously run
   */
  @Override
  public int getExecutorCapacity() {
    return resourceSpecification.getSlot();
  }

  /**
   * @return the current snapshot of set of Tasks that are running in this executor.
   */
  @Override
  public synchronized Set<Task> getScheduledTasks() {
    return scheduledTaskToAttempt.keySet();
  }

  @Override
  public synchronized Set<Task> getRunningTasks() {
    return runningTasks;
  }

  /**
   * @return the number of running {@link Task}s.
   */
  @Override
  public int getNumOfRunningTasks() {
    return getNumOfComplyingRunningTasks() + getNumOfNonComplyingRunningTasks();
  }

  /**
   * @return the number of running {@link Task}s that complies to the executor slot restriction.
   */
  @Override
  public synchronized int getNumOfComplyingRunningTasks() {
    return runningComplyingTasks.size();
  }

  /**
   * @return the number of running {@link Task}s that does not comply to the executor slot restriction.
   */
  @Override
  public synchronized int getNumOfNonComplyingRunningTasks() {
    return runningNonComplyingTasks.size();
  }

  /**
   * @return the executor id
   */
  @Override
  public String getExecutorId() {
    return executorId;
  }

  /**
   * @return the container type
   */
  @Override
  public String getContainerType() {
    return resourceSpecification.getContainerType();
  }

  /**
   * @return physical name of the node where this executor resides
   */
  @Override
  public String getNodeName() {
    return nodeName;
  }

  /**
   * Shuts down this executor.
   */
  @Override
  public void shutDown() {
    executorShutdownHandler.close();
  }

  @Override
  public String toString() {
    final ObjectMapper mapper = new ObjectMapper();
    final ObjectNode node = mapper.createObjectNode();
    node.put("executorId", executorId);
    node.put("scheduledTasks", getScheduledTasks().toString());
    node.put("runningTasks", getRunningTasks().toString());
    node.put("failedTasks", failedTasks.toString());
    return node.toString();
  }

  /**
   * Removes the specified {@link Task} from the map of running tasks.
   *
   * @param taskId id of the task to remove
   * @return the removed {@link Task}
   */
  private Task removeFromRunningTasks(final String taskId) {
    final Task task;
    scheduledTaskToAttempt.remove(taskId);
    if (runningComplyingTasks.containsKey(taskId)) {
      task = runningComplyingTasks.remove(taskId);
    } else if (runningNonComplyingTasks.containsKey(taskId)) {
      task = runningNonComplyingTasks.remove(taskId);
    } else {
      throw new RuntimeException(String.format("Task %s not found in its DefaultExecutorRepresenterImpl", taskId));
    }
    return task;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DefaultExecutorRepresenterImpl that = (DefaultExecutorRepresenterImpl) o;
    return Objects.equals(executorId, that.executorId);
  }

  @Override
  public int hashCode() {

    return Objects.hash(executorId);
  }
}

