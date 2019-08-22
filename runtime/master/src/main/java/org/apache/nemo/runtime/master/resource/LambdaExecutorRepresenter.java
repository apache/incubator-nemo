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

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.Gson;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.util.CharsetUtil;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourceSlotProperty;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.master.LambdaMaster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * LambdaExecutorRepresenter.
 */
public class LambdaExecutorRepresenter implements ExecutorRepresenter {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultExecutorRepresenter.class.getName());

  private final String executorId;
  private final Map<String, Task> runningComplyingTasks;
  private final Map<String, Task> runningNonComplyingTasks;
  private final Map<Task, Integer> runningTaskToAttempt;
  private final Set<Task> completeTasks;
  private final Set<Task> failedTasks;
  private final String nodeName;

  private final AWSLambda client;
  private final String lambdaFunctionName;
  private final LambdaMaster lambdaMaster;

  /**
   * Creates a reference to the specified executor.
   *
   * @param executorId                   the executor id
   * @param nodeName                     physical name of the node where this executor resides
   */
  public LambdaExecutorRepresenter(final String executorId,
                                   final LambdaMaster lambdaMaster,
                                   final String nodeName) {
    this.executorId = executorId;

    this.runningComplyingTasks = new HashMap<>();
    this.runningNonComplyingTasks = new HashMap<>();
    this.runningTaskToAttempt = new HashMap<>();
    this.completeTasks = new HashSet<>();
    this.failedTasks = new HashSet<>();
    this.nodeName = nodeName;
    this.lambdaMaster = lambdaMaster;

    // Need a function reading user parameters such as lambda function name etc.
    Regions region = Regions.fromName("ap-northeast-1");
    this.client = AWSLambdaClientBuilder.standard().withRegion(region).build();
    this.lambdaFunctionName = "lambda-dev-executor";
  }

  /**
   * Marks all Tasks which were running in this executor as failed.
   *
   * @return set of identifiers of tasks that were running in this executor.
   */
  @Override
  public Set<String> onExecutorFailed() {
    LOG.info("##### Lambda Executor Failed: " + this.toString());
    throw new UnsupportedOperationException();
  }

  /**
   * Marks the Task as running, and sends scheduling message to the executor.
   *
   * @param task the task to run
   */
  @Override
  public void onTaskScheduled(final Task task) {
    System.out.println("onTaskScheduled: " + task.getTaskId());
    (task.getPropertyValue(ResourceSlotProperty.class).orElse(true)
      ? runningComplyingTasks : runningNonComplyingTasks).put(task.getTaskId(), task);
    runningTaskToAttempt.put(task, task.getAttemptIdx());

    // remove from failedTasks if this task failed before
    failedTasks.remove(task);

    // serialize this task
    // write this task to the executor
    Channel channel = this.lambdaMaster.getExecutorChannel();
    System.out.println("onTaskScheduled write to this channel " + channel);
    final byte[] serialized = SerializationUtils.serialize(task);
    System.out.println(serialized.toString());
    channel.writeAndFlush(new LambdaEvent(LambdaEvent.Type.WORKER_INIT, serialized, serialized.length));

    System.out.println("Dispatch task to LambdaExecutor taskId: " + task.getTaskId());
  }

  /**
   * Sends control message to the executor.
   *
   * @param message Message object to send
   */
  @Override
  public void sendControlMessage(final ControlMessage.Message message) {
    if (message.getType() == ControlMessage.MessageType.RequestMetricFlush) {
      throw new UnsupportedOperationException();
    } else {
      LOG.info("##### Lambda Executor control msg not supported " + message.getType());
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Marks the specified Task as completed.
   *
   * @param taskId id of the completed task
   */
  @Override
  public void onTaskExecutionComplete(final String taskId) {
    final Task completedTask = removeFromRunningTasks(taskId);
    runningTaskToAttempt.remove(completedTask);
    completeTasks.add(completedTask);
  }

  /**
   * Marks the specified Task as failed.
   *
   * @param taskId id of the Task
   */
  @Override
  public void onTaskExecutionFailed(final String taskId) {
    final Task failedTask = removeFromRunningTasks(taskId);
    runningTaskToAttempt.remove(failedTask);
    failedTasks.add(failedTask);
  }

  /**
   * @return how many Tasks can this executor simultaneously run
   */
  @Override
  public int getExecutorCapacity() {
    return 1;
  }

  /**
   * @return the current snapshot of set of Tasks that are running in this executor.
   */
  @Override
  public Set<Task> getRunningTasks() {
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
  public int getNumOfComplyingRunningTasks() {
    return runningComplyingTasks.size();
  }

  /**
   * @return the number of running {@link Task}s that does not comply to the executor slot restriction.
   */
  private int getNumOfNonComplyingRunningTasks() {
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
    return "";
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
  }

  /**
   * toString.
   */
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
      throw new RuntimeException(String.format("Task %s not found in its DefaultExecutorRepresenter", taskId));
    }
    return task;
  }
}
