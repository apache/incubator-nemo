/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.runtime.master.resource;

import com.google.protobuf.ByteString;
import edu.snu.nemo.common.ir.vertex.executionproperty.ResourceSlotProperty;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.comm.ControlMessage;
import edu.snu.nemo.runtime.common.message.MessageEnvironment;
import edu.snu.nemo.runtime.common.message.MessageSender;
import edu.snu.nemo.runtime.common.plan.Task;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.reef.driver.context.ActiveContext;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
public final class ExecutorRepresenter {

  private final String executorId;
  private final ResourceSpecification resourceSpecification;
  private final Map<String, Task> runningComplyingTasks;
  private final Map<String, Task> runningNonComplyingTasks;
  private final Map<Task, Integer> runningTaskToAttempt;
  private final Set<Task> completeTasks;
  private final Set<Task> failedTasks;
  private final MessageSender<ControlMessage.Message> messageSender;
  private final ActiveContext activeContext;
  private final ExecutorService serializationExecutorService;
  private final String nodeName;

  /**
   * Creates a reference to the specified executor.
   * @param executorId the executor id
   * @param resourceSpecification specification for the executor
   * @param messageSender provides communication context for this executor
   * @param activeContext context on the corresponding REEF evaluator
   * @param serializationExecutorService provides threads for message serialization
   * @param nodeName physical name of the node where this executor resides
   */
  public ExecutorRepresenter(final String executorId,
                             final ResourceSpecification resourceSpecification,
                             final MessageSender<ControlMessage.Message> messageSender,
                             final ActiveContext activeContext,
                             final ExecutorService serializationExecutorService,
                             final String nodeName) {
    this.executorId = executorId;
    this.resourceSpecification = resourceSpecification;
    this.messageSender = messageSender;
    this.runningComplyingTasks = new HashMap<>();
    this.runningNonComplyingTasks = new HashMap<>();
    this.runningTaskToAttempt = new HashMap<>();
    this.completeTasks = new HashSet<>();
    this.failedTasks = new HashSet<>();
    this.activeContext = activeContext;
    this.serializationExecutorService = serializationExecutorService;
    this.nodeName = nodeName;
  }

  /**
   * Marks all Tasks which were running in this executor as failed.
   */
  public Set<String> onExecutorFailed() {
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
   * @param task
   */
  public void onTaskScheduled(final Task task) {
    (task.getPropertyValue(ResourceSlotProperty.class).orElse(true)
        ? runningComplyingTasks : runningNonComplyingTasks).put(task.getTaskId(), task);
    runningTaskToAttempt.put(task, task.getAttemptIdx());
    failedTasks.remove(task);

    serializationExecutorService.submit(() -> {
      final byte[] serialized = SerializationUtils.serialize(task);
      sendControlMessage(
          ControlMessage.Message.newBuilder()
              .setId(RuntimeIdGenerator.generateMessageId())
              .setListenerId(MessageEnvironment.EXECUTOR_MESSAGE_LISTENER_ID)
              .setType(ControlMessage.MessageType.ScheduleTask)
              .setScheduleTaskMsg(
                  ControlMessage.ScheduleTaskMsg.newBuilder()
                      .setTask(ByteString.copyFrom(serialized))
                      .build())
              .build());
    });
  }

  /**
   * Sends control message to the executor.
   * @param message Message object to send
   */
  public void sendControlMessage(final ControlMessage.Message message) {
    messageSender.send(message);
  }

  /**
   * Marks the specified Task as completed.
   *
   */
  public void onTaskExecutionComplete(final String taskId) {
    final Task completedTask = removeFromRunningTasks(taskId);
    runningTaskToAttempt.remove(completedTask);
    completeTasks.add(completedTask);
  }

  /**
   * Marks the specified Task as failed.
   * @param taskId id of the Task
   */
  public void onTaskExecutionFailed(final String taskId) {
    final Task failedTask = removeFromRunningTasks(taskId);
    runningTaskToAttempt.remove(failedTask);
    failedTasks.add(failedTask);
  }

  /**
   * @return how many Tasks can this executor simultaneously run
   */
  public int getExecutorCapacity() {
    return resourceSpecification.getCapacity();
  }

  /**
   * @return the current snapshot of set of Tasks that are running in this executor.
   */
  public Set<Task> getRunningTasks() {
    return Stream.concat(runningComplyingTasks.values().stream(),
        runningNonComplyingTasks.values().stream()).collect(Collectors.toSet());
  }

  /**
   * @return the number of running {@link Task}s.
   */
  public int getNumOfRunningTasks() {
    return getNumOfComplyingRunningTasks() + getNumOfNonComplyingRunningTasks();
  }

  /**
   * @return the number of running {@link Task}s that complies to the executor slot restriction.
   */
  public int getNumOfComplyingRunningTasks() {
    return runningComplyingTasks.size();
  }

  /**
   * @return the number of running {@link Task}s that does not comply to the executor slot restriction.
   */
  public int getNumOfNonComplyingRunningTasks() {
    return runningNonComplyingTasks.size();
  }

  /**
   * @return the executor id
   */
  public String getExecutorId() {
    return executorId;
  }

  /**
   * @return the container type
   */
  public String getContainerType() {
    return resourceSpecification.getContainerType();
  }

  /**
   * @return physical name of the node where this executor resides
   */
  public String getNodeName() {
    return nodeName;
  }

  /**
   * Shuts down this executor.
   */
  public void shutDown() {
    activeContext.close();
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("ExecutorRepresenter{");
    sb.append("executorId='").append(executorId).append('\'');
    sb.append(", runningTasks=").append(getRunningTasks());
    sb.append(", failedTasks=").append(failedTasks);
    sb.append('}');
    return sb.toString();
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
      throw new RuntimeException(String.format("Task %s not found in its ExecutorRepresenter", taskId));
    }
    return task;
  }
}

