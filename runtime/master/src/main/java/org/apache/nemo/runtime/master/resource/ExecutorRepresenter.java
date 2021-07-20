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

import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.plan.Task;

import java.util.Set;

/**
 * Contains information/state regarding an executor
 * Such information may include
 * a) The executor's resource type.
 * b) The executor's capacity (ex. number of cores).
 * c) Tasks scheduled/launched for the executor.
 * d) Name of the physical node which hosts this executor.
 * e) (Please add other information as we implement more features).
 */
public interface ExecutorRepresenter {

  /**
   * Marks all Tasks which were running in this executor as failed.
   *
   * @return set of identifiers of tasks that were running in this executor.
   */
  Set<String> onExecutorFailed();

  /**
   * @return how many Tasks can this executor simultaneously run
   */
  int getExecutorCapacity();

  /**
   * @return the current snapshot of set of Tasks that are running in this executor.
   */
  Set<Task> getRunningTasks();

  /**
   * @return the number of running {@link Task}s.
   */
  int getNumOfRunningTasks();

  /**
   * @return the number of running {@link Task}s that complies to the executor slot restriction.
   */
  int getNumOfComplyingRunningTasks();

  /**
   * Marks the Task as running, and sends scheduling message to the executor.
   *
   * @param task the task to run
   */
   void onTaskScheduled(Task task);

  /**
   * Sends control message to the executor.
   *
   * @param message Message object to send
   */
   void sendControlMessage(ControlMessage.Message message);

  /**
   * Marks the specified Task as completed.
   *
   * @param taskId id of the completed task
   */
  void onTaskExecutionComplete(String taskId);

  /**
   * @return physical name of the node where this executor resides
   */
  String getNodeName();

  /**
   * @return the executor id
   */
  String getExecutorId();

  /**
   * @return the container type
   */
  String getContainerType();

  /**
   * Shuts down this executor.
   */
  void shutDown();

  /**
   * Marks the specified Task as failed.
   *
   * @param taskId id of the Task
   */
  void onTaskExecutionFailed(String taskId);

  /**
   * @return true if this executor has an available slot.
   */
  boolean isExecutorSlotAvailable();
}
