/*
 * Copyright (C) 2017 Seoul National University
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

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.exception.UnknownExecutionStateException;
import edu.snu.nemo.common.exception.UnknownFailureCauseException;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.comm.ControlMessage;
import edu.snu.nemo.runtime.common.message.MessageEnvironment;
import edu.snu.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import edu.snu.nemo.runtime.common.plan.RuntimeEdge;
import edu.snu.nemo.runtime.common.plan.physical.ScheduledTaskGroup;
import edu.snu.nemo.runtime.common.plan.physical.Task;
import edu.snu.nemo.runtime.common.state.TaskGroupState;

import java.util.*;

import org.apache.reef.annotations.audience.EvaluatorSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the states related to a task group.
 * The methods of this class are synchronized.
 */
@EvaluatorSide
public final class TaskGroupStateManager {
  private static final Logger LOG = LoggerFactory.getLogger(TaskGroupStateManager.class.getName());

  private final String taskGroupId;
  private final int attemptIdx;
  private final String executorId;
  private final MetricCollector metricCollector;
  private final PersistentConnectionToMasterMap persistentConnectionToMasterMap;

  public TaskGroupStateManager(final ScheduledTaskGroup scheduledTaskGroup,
                               final DAG<Task, RuntimeEdge<Task>> taskGroupDag,
                               final String executorId,
                               final PersistentConnectionToMasterMap persistentConnectionToMasterMap,
                               final MetricMessageSender metricMessageSender) {
    this.taskGroupId = scheduledTaskGroup.getTaskGroupId();
    this.attemptIdx = scheduledTaskGroup.getAttemptIdx();
    this.executorId = executorId;
    this.persistentConnectionToMasterMap = persistentConnectionToMasterMap;
    this.metricCollector = new MetricCollector(metricMessageSender);
  }

  /**
   * Updates the state of the task group.
   * @param newState of the task group.
   * @param taskPutOnHold the logical ID of the tasks put on hold, empty otherwise.
   * @param cause only provided as non-empty upon recoverable failures.
   */
  public synchronized void onTaskGroupStateChanged(final TaskGroupState.State newState,
                                                   final Optional<String> taskPutOnHold,
                                                   final Optional<TaskGroupState.RecoverableFailureCause> cause) {
    final Map<String, Object> metric = new HashMap<>();

    switch (newState) {
      case EXECUTING:
        LOG.debug("Executing TaskGroup ID {}...", this.taskGroupId);
        metric.put("ContainerId", executorId);
        metric.put("ScheduleAttempt", attemptIdx);
        metric.put("FromState", newState);
        metricCollector.beginMeasurement(taskGroupId, metric);
        break;
      case COMPLETE:
        LOG.debug("TaskGroup ID {} complete!", this.taskGroupId);
        metric.put("ToState", newState);
        metricCollector.endMeasurement(taskGroupId, metric);
        notifyTaskGroupStateToMaster(newState, Optional.empty(), cause);
        break;
      case FAILED_RECOVERABLE:
        LOG.debug("TaskGroup ID {} failed (recoverable).", this.taskGroupId);
        metric.put("ToState", newState);
        metricCollector.endMeasurement(taskGroupId, metric);
        notifyTaskGroupStateToMaster(newState, Optional.empty(), cause);
        break;
      case FAILED_UNRECOVERABLE:
        LOG.debug("TaskGroup ID {} failed (unrecoverable).", this.taskGroupId);
        metric.put("ToState", newState);
        metricCollector.endMeasurement(taskGroupId, metric);
        notifyTaskGroupStateToMaster(newState, Optional.empty(), cause);
        break;
      case ON_HOLD:
        LOG.debug("TaskGroup ID {} put on hold.", this.taskGroupId);
        notifyTaskGroupStateToMaster(newState, taskPutOnHold, cause);
        break;
      default:
        throw new IllegalStateException("Illegal state at this point");
    }
  }

  /**
   * Notifies the change in task group state to master.
   * @param newState of the task group.
   * @param taskPutOnHold the logical ID of the tasks put on hold, empty otherwise.
   * @param cause only provided as non-empty upon recoverable failures.
   */
  private void notifyTaskGroupStateToMaster(final TaskGroupState.State newState,
                                            final Optional<String> taskPutOnHold,
                                            final Optional<TaskGroupState.RecoverableFailureCause> cause) {
    final ControlMessage.TaskGroupStateChangedMsg.Builder msgBuilder =
        ControlMessage.TaskGroupStateChangedMsg.newBuilder()
          .setExecutorId(executorId)
          .setTaskGroupId(taskGroupId)
          .setAttemptIdx(attemptIdx)
          .setState(convertState(newState));
    if (taskPutOnHold.isPresent()) {
          msgBuilder.setTaskPutOnHoldId(taskPutOnHold.get());
    }
    if (cause.isPresent()) {
      msgBuilder.setFailureCause(convertFailureCause(cause.get()));
    }

    // Send taskGroupStateChangedMsg to master!
    persistentConnectionToMasterMap.getMessageSender(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID).send(
        ControlMessage.Message.newBuilder()
            .setId(RuntimeIdGenerator.generateMessageId())
            .setListenerId(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID)
            .setType(ControlMessage.MessageType.TaskGroupStateChanged)
            .setTaskGroupStateChangedMsg(msgBuilder.build())
            .build());
  }

  private ControlMessage.TaskGroupStateFromExecutor convertState(final TaskGroupState.State state) {
    switch (state) {
    case READY:
      return ControlMessage.TaskGroupStateFromExecutor.READY;
    case EXECUTING:
      return ControlMessage.TaskGroupStateFromExecutor.EXECUTING;
    case COMPLETE:
      return ControlMessage.TaskGroupStateFromExecutor.COMPLETE;
    case FAILED_RECOVERABLE:
      return ControlMessage.TaskGroupStateFromExecutor.FAILED_RECOVERABLE;
    case FAILED_UNRECOVERABLE:
      return ControlMessage.TaskGroupStateFromExecutor.FAILED_UNRECOVERABLE;
    case ON_HOLD:
      return ControlMessage.TaskGroupStateFromExecutor.ON_HOLD;
    default:
      throw new UnknownExecutionStateException(new Exception("This TaskGroupState is unknown: " + state));
    }
  }

  private ControlMessage.RecoverableFailureCause convertFailureCause(
    final TaskGroupState.RecoverableFailureCause cause) {
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
  public void getCurrentTaskGroupExecutionState() {
  }
}
