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
package edu.snu.coral.runtime.executor;

import edu.snu.coral.common.dag.DAG;
import edu.snu.coral.common.exception.UnknownExecutionStateException;
import edu.snu.coral.common.exception.UnknownFailureCauseException;
import edu.snu.coral.runtime.common.RuntimeIdGenerator;
import edu.snu.coral.runtime.common.comm.ControlMessage;
import edu.snu.coral.runtime.common.message.MessageEnvironment;
import edu.snu.coral.runtime.common.message.PersistentConnectionToMasterMap;
import edu.snu.coral.runtime.common.plan.RuntimeEdge;
import edu.snu.coral.runtime.common.plan.physical.ScheduledTaskGroup;
import edu.snu.coral.runtime.common.plan.physical.Task;
import edu.snu.coral.runtime.common.state.TaskGroupState;
import edu.snu.coral.runtime.common.state.TaskState;
import edu.snu.coral.runtime.common.metric.MetricDataBuilder;

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
  private final MetricMessageSender metricMessageSender;
  private final Map<String, MetricDataBuilder> metricDataBuilderMap;

  /**
   * Used to track all task states of this task group, by keeping a map of logical task ids to their states.
   */
  private final Map<String, TaskState> logicalIdToTaskStates;

  /**
   * Used to track task group completion status.
   * All task ids are added to the set when the this task group begins executing.
   * Each task id is removed upon completion,
   * therefore indicating the task group's completion when this set becomes empty.
   */
  private Set<String> currentTaskGroupTaskIds;

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
    this.metricMessageSender = metricMessageSender;
    metricDataBuilderMap = new HashMap<>();
    logicalIdToTaskStates = new HashMap<>();
    currentTaskGroupTaskIds = new HashSet<>();
    initializeStates(taskGroupDag);
  }

  /**
   * Receives and initializes the states for the task group to manage.
   * @param taskGroupDag to manage.
   */
  private void initializeStates(final DAG<Task, RuntimeEdge<Task>> taskGroupDag) {
    taskGroupDag.getVertices().forEach(task -> {
      currentTaskGroupTaskIds.add(task.getId());
      logicalIdToTaskStates.put(task.getId(), new TaskState());
    });
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
      beginMeasurement(taskGroupId, metric);
      logicalIdToTaskStates.forEach((taskId, state) -> {
        LOG.debug("Task State Transition: id {} from {} to {}",
            taskId, state.getStateMachine().getCurrentState(), TaskState.State.PENDING_IN_EXECUTOR);
        state.getStateMachine().setState(TaskState.State.PENDING_IN_EXECUTOR);
      });
      break;
    case COMPLETE:
      LOG.debug("TaskGroup ID {} complete!", this.taskGroupId);
      metric.put("ToState", newState);
      endMeasurement(taskGroupId, metric);
      notifyTaskGroupStateToMaster(newState, Optional.empty(), cause);
      break;
    case FAILED_RECOVERABLE:
      LOG.debug("TaskGroup ID {} failed (recoverable).", this.taskGroupId);
      /*
      metric.put("ToState", newState);
      endMeasurement(taskGroupId, metric);
      */
      notifyTaskGroupStateToMaster(newState, Optional.empty(), cause);
      break;
    case FAILED_UNRECOVERABLE:
      LOG.debug("TaskGroup ID {} failed (unrecoverable).", this.taskGroupId);
      /*
      metric.put("ToState", newState);
      endMeasurement(taskGroupId, metric);
      */
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

  // TODO #164: Cleanup Protobuf Usage
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

  // TODO #164: Cleanup Protobuf Usage
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

  /**
   * Begins recording the start time of this metric measurement, in addition to the metric given.
   * This method ensures thread-safety by synchronizing its callers.
   * @param compUnitId to be used as metricKey
   * @param initialMetric metric to add
   */
  private void beginMeasurement(final String compUnitId, final Map<String, Object> initialMetric) {
    final MetricDataBuilder metricDataBuilder = new MetricDataBuilder(compUnitId);
    metricDataBuilder.beginMeasurement(initialMetric);
    metricDataBuilderMap.put(compUnitId, metricDataBuilder);
  }

  /**
   * Ends this metric measurement, recording the end time in addition to the metric given.
   * This method ensures thread-safety by synchronizing its callers.
   * @param compUnitId to be used as metricKey
   * @param finalMetric metric to add
   */
  private void endMeasurement(final String compUnitId, final Map<String, Object> finalMetric) {
    final MetricDataBuilder metricDataBuilder = metricDataBuilderMap.get(compUnitId);
    metricDataBuilder.endMeasurement(finalMetric);
    metricMessageSender.send(compUnitId, metricDataBuilder.build().toJson());
    metricDataBuilderMap.remove(compUnitId);
  }

  // Tentative
  public void getCurrentTaskGroupExecutionState() {

  }
}
