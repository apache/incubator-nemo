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
package org.apache.nemo.runtime.executor;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.MessageUtils;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.runtime.common.metric.StateTransitionEvent;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.common.state.TaskState;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Manages the states related to a task.
 * The methods of this class are synchronized.
 */
@EvaluatorSide
public final class TaskStateManager {
  private static final Logger LOG = LoggerFactory.getLogger(TaskStateManager.class.getName());

  private final String taskId;
  private final int attemptIdx;
  private final String executorId;
  private final MetricMessageSender metricMessageSender;
  private final PersistentConnectionToMasterMap persistentConnectionToMasterMap;

  private static final String METRIC = "TaskMetric";

  public TaskStateManager(final Task task,
                          final String executorId,
                          final PersistentConnectionToMasterMap persistentConnectionToMasterMap,
                          final MetricMessageSender metricMessageSender) {
    this.taskId = task.getTaskId();
    this.attemptIdx = task.getAttemptIdx();
    this.executorId = executorId;
    this.persistentConnectionToMasterMap = persistentConnectionToMasterMap;
    this.metricMessageSender = metricMessageSender;

    metricMessageSender.send(METRIC, taskId, "containerId", SerializationUtils.serialize(executorId));
    metricMessageSender.send(METRIC, taskId, "scheduleAttempt", SerializationUtils.serialize(attemptIdx));
  }

  /**
   * Updates the state of the task.
   *
   * @param newState        of the task.
   * @param vertexPutOnHold the vertex put on hold.
   * @param cause           only provided as non-empty upon recoverable failures.
   */
  public synchronized void onTaskStateChanged(final TaskState.State newState,
                                              final Optional<String> vertexPutOnHold,
                                              final Optional<TaskState.RecoverableTaskFailureCause> cause) {
    metricMessageSender.send(METRIC, taskId, "stateTransitionEvent",
      SerializationUtils.serialize(new StateTransitionEvent<>(System.currentTimeMillis(), null, newState)));

    switch (newState) {
      case EXECUTING:
        LOG.debug("Executing Task ID {}...", this.taskId);
        break;
      case COMPLETE:
        LOG.debug("Task ID {} complete!", this.taskId);
        notifyTaskStateToMaster(newState, Optional.empty(), cause);
        break;
      case SHOULD_RETRY:
        LOG.debug("Task ID {} failed (recoverable).", this.taskId);
        notifyTaskStateToMaster(newState, Optional.empty(), cause);
        break;
      case FAILED:
        LOG.debug("Task ID {} failed (unrecoverable).", this.taskId);
        notifyTaskStateToMaster(newState, Optional.empty(), cause);
        break;
      case ON_HOLD:
        LOG.debug("Task ID {} put on hold.", this.taskId);
        notifyTaskStateToMaster(newState, vertexPutOnHold, cause);
        break;
      default:
        throw new IllegalStateException("Illegal state at this point");
    }
  }

  /**
   * Notifies the change in task state to master.
   *
   * @param newState        of the task.
   * @param vertexPutOnHold the vertex put on hold.
   * @param cause           only provided as non-empty upon recoverable failures.
   */
  private void notifyTaskStateToMaster(final TaskState.State newState,
                                       final Optional<String> vertexPutOnHold,
                                       final Optional<TaskState.RecoverableTaskFailureCause> cause) {
    final ControlMessage.TaskStateChangedMsg.Builder msgBuilder =
      ControlMessage.TaskStateChangedMsg.newBuilder()
        .setExecutorId(executorId)
        .setTaskId(taskId)
        .setAttemptIdx(attemptIdx)
        .setState(MessageUtils.convertState(newState));
    vertexPutOnHold.ifPresent(msgBuilder::setVertexPutOnHoldId);
    cause.ifPresent(c -> msgBuilder.setFailureCause(MessageUtils.convertFailureCause(c)));

    // Send taskStateChangedMsg to master!
    persistentConnectionToMasterMap.getMessageSender(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID).send(
      ControlMessage.Message.newBuilder()
        .setId(RuntimeIdManager.generateMessageId())
        .setListenerId(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID)
        .setType(ControlMessage.MessageType.TaskStateChanged)
        .setTaskStateChangedMsg(msgBuilder.build())
        .build());
  }

  // Tentative
  public void getCurrentTaskExecutionState() {
  }
}
