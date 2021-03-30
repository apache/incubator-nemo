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
import org.apache.nemo.runtime.master.ExecutorRepresenter;
import org.apache.nemo.runtime.master.ExecutorShutdownHandler;
import org.apache.nemo.runtime.master.SerializedTaskMap;
import org.apache.nemo.runtime.message.MessageSender;
import org.apache.reef.driver.context.ActiveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
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
  private final Map<Task, Integer> runningTaskToAttempt;
  private final Set<Task> completeTasks;
  private final Set<Task> failedTasks;
  private final MessageSender<ControlMessage.Message> messageSender;
  private final ExecutorShutdownHandler executorShutdownHandler;
  private final ExecutorService serializationExecutorService;
  private final String nodeName;
  private final SerializedTaskMap serializedTaskMap;

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
    this.runningTaskToAttempt = new ConcurrentHashMap<>();
    this.completeTasks = new HashSet<>();
    this.failedTasks = new HashSet<>();
    this.executorShutdownHandler = executorShutdownHandler;
    this.serializationExecutorService = serializationExecutorService;
    this.nodeName = nodeName;
    this.serializedTaskMap = serializedTaskMap;
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

  /**
   * Marks the Task as running, and sends scheduling message to the executor.
   * @param task the task to run
   */
  @Override
  public synchronized void onTaskScheduled(final Task task) {
    (task.getPropertyValue(ResourceSlotProperty.class).orElse(true)
        ? runningComplyingTasks : runningNonComplyingTasks).put(task.getTaskId(), task);
    runningTaskToAttempt.put(task, task.getAttemptIdx());
    failedTasks.remove(task);

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
    });
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
    runningTaskToAttempt.remove(completedTask);
    completeTasks.add(completedTask);
  }

  /**
   * Marks the specified Task as completed.
   * @param taskId id of the completed task
   */
  @Override
  public void onTaskExecutionStop(final String taskId) {
    final Task completedTask = removeFromRunningTasks(taskId);
    runningTaskToAttempt.remove(completedTask);
  }

  /**
   * Marks the specified Task as failed.
   * @param taskId id of the Task
   */
  @Override
  public synchronized void onTaskExecutionFailed(final String taskId) {
    final Task failedTask = removeFromRunningTasks(taskId);
    runningTaskToAttempt.remove(failedTask);
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
  public synchronized Set<Task> getRunningTasks() {
    return Stream.concat(runningComplyingTasks.values().stream(),
        runningNonComplyingTasks.values().stream()).collect(Collectors.toSet());
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
