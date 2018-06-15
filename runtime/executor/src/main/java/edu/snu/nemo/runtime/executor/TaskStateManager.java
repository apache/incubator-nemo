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
package edu.snu.nemo.runtime.executor;

import edu.snu.nemo.common.exception.UnknownExecutionStateException;
import edu.snu.nemo.common.exception.UnknownFailureCauseException;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.comm.ControlMessage;
import edu.snu.nemo.runtime.common.message.MessageEnvironment;
import edu.snu.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import edu.snu.nemo.runtime.common.plan.Task;

import java.util.*;

import edu.snu.nemo.runtime.common.state.TaskState;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private final MetricCollector metricCollector;
  private final PersistentConnectionToMasterMap persistentConnectionToMasterMap;

  public TaskStateManager(final Task task,
                          final String executorId,
                          final PersistentConnectionToMasterMap persistentConnectionToMasterMap,
                          final MetricMessageSender metricMessageSender) {
    this.taskId = task.getTaskId();
    this.attemptIdx = task.getAttemptIdx();
    this.executorId = executorId;
    this.persistentConnectionToMasterMap = persistentConnectionToMasterMap;
    this.metricCollector = new MetricCollector(metricMessageSender);
  }

  /**
   * Updates the state of the task.
   * @param newState of the task.
   * @param vertexPutOnHold the vertex put on hold.
   * @param cause only provided as non-empty upon recoverable failures.
   */
  public synchronized void onTaskStateChanged(final TaskState.State newState,
                                              final Optional<String> vertexPutOnHold,
                                              final Optional<TaskState.RecoverableFailureCause> cause) {
    final Map<String, Object> metric = new HashMap<>();

    switch (newState) {
      case EXECUTING:
        LOG.debug("Executing Task ID {}...", this.taskId);
        metric.put("ContainerId", executorId);
        metric.put("ScheduleAttempt", attemptIdx);
        metric.put("FromState", newState);
        metricCollector.beginMeasurement(taskId, metric);
        break;
      case COMPLETE:
        LOG.debug("Task ID {} complete!", this.taskId);
        metric.put("ToState", newState);
        metricCollector.endMeasurement(taskId, metric);
        notifyTaskStateToMaster(newState, Optional.empty(), cause);
        break;
      case FAILED_RECOVERABLE:
        LOG.debug("Task ID {} failed (recoverable).", this.taskId);
        metric.put("ToState", newState);
        metricCollector.endMeasurement(taskId, metric);
        notifyTaskStateToMaster(newState, Optional.empty(), cause);
        break;
      case FAILED_UNRECOVERABLE:
        LOG.debug("Task ID {} failed (unrecoverable).", this.taskId);
        metric.put("ToState", newState);
        metricCollector.endMeasurement(taskId, metric);
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
   * @param newState of the task.
   * @param vertexPutOnHold the vertex put on hold.
   * @param cause only provided as non-empty upon recoverable failures.
   */
  private void notifyTaskStateToMaster(final TaskState.State newState,
                                       final Optional<String> vertexPutOnHold,
                                       final Optional<TaskState.RecoverableFailureCause> cause) {
    final ControlMessage.TaskStateChangedMsg.Builder msgBuilder =
        ControlMessage.TaskStateChangedMsg.newBuilder()
            .setExecutorId(executorId)
            .setTaskId(taskId)
            .setAttemptIdx(attemptIdx)
            .setState(convertState(newState));
    if (vertexPutOnHold.isPresent()) {
      msgBuilder.setVertexPutOnHoldId(vertexPutOnHold.get());
    }
    if (cause.isPresent()) {
      msgBuilder.setFailureCause(convertFailureCause(cause.get()));
    }

    // Send taskStateChangedMsg to master!
    persistentConnectionToMasterMap.getMessageSender(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID).send(
        ControlMessage.Message.newBuilder()
            .setId(RuntimeIdGenerator.generateMessageId())
            .setListenerId(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID)
            .setType(ControlMessage.MessageType.TaskStateChanged)
            .setTaskStateChangedMsg(msgBuilder.build())
            .build());
  }

  private ControlMessage.TaskStateFromExecutor convertState(final TaskState.State state) {
    switch (state) {
      case READY:
        return ControlMessage.TaskStateFromExecutor.READY;
      case EXECUTING:
        return ControlMessage.TaskStateFromExecutor.EXECUTING;
      case COMPLETE:
        return ControlMessage.TaskStateFromExecutor.COMPLETE;
      case FAILED_RECOVERABLE:
        return ControlMessage.TaskStateFromExecutor.FAILED_RECOVERABLE;
      case FAILED_UNRECOVERABLE:
        return ControlMessage.TaskStateFromExecutor.FAILED_UNRECOVERABLE;
      case ON_HOLD:
        return ControlMessage.TaskStateFromExecutor.ON_HOLD;
      default:
        throw new UnknownExecutionStateException(new Exception("This TaskState is unknown: " + state));
    }
  }

  private ControlMessage.RecoverableFailureCause convertFailureCause(
      final TaskState.RecoverableFailureCause cause) {
    switch (cause) {
      case INPUT_READ_FAILURE:
        return ControlMessage.RecoverableFailureCause.InputReadFailure;
      case OUTPUT_WRITE_FAILURE:
        return ControlMessage.RecoverableFailureCause.OutputWriteFailure;
      default:
        throw new UnknownFailureCauseException(
            new Throwable("The failure cause for the recoverable failure is unknown"));
    }
  }

  // Tentative
  public void getCurrentTaskExecutionState() {
  }
}
