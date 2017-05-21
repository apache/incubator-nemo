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
package edu.snu.vortex.runtime.executor;

import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.common.plan.physical.TaskGroup;
import edu.snu.vortex.runtime.common.state.TaskGroupState;
import edu.snu.vortex.runtime.common.state.TaskState;
import edu.snu.vortex.runtime.exception.UnknownExecutionStateException;
import edu.snu.vortex.utils.StateMachine;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages the states related to a task group.
 * The methods of this class are synchronized.
 */
// TODO #163: Handle Fault Tolerance
public final class TaskGroupStateManager {
  private static final Logger LOG = Logger.getLogger(TaskGroupStateManager.class.getName());

  private final String taskGroupId;
  private final String executorId;

  /**
   * Used to track all task states of this task group, by keeping a map of task ids to their states.
   */
  private final Map<String, TaskState> idToTaskStates;

  /**
   * Used to track task group completion status.
   * All task ids are added to the set when the this task group begins executing.
   * Each task id is removed upon completion,
   * therefore indicating the task group's completion when this set becomes empty.
   */
  private Set<String> currentTaskGroupTaskIds;

  private final PersistentConnectionToMaster persistentConnectionToMaster;


  public TaskGroupStateManager(final TaskGroup taskGroup,
                               final String executorId,
                               final PersistentConnectionToMaster persistentConnectionToMaster) {
    this.taskGroupId = taskGroup.getTaskGroupId();
    this.executorId = executorId;
    this.persistentConnectionToMaster = persistentConnectionToMaster;
    idToTaskStates = new HashMap<>();
    currentTaskGroupTaskIds = new HashSet<>();
    initializeStates(taskGroup);
  }

  /**
   * Receives and initializes the states for the task group to manage.
   * @param taskGroup to manage.
   */
  private void initializeStates(final TaskGroup taskGroup) {
    onTaskGroupStateChanged(TaskGroupState.State.EXECUTING, Optional.empty());

    taskGroup.getTaskDAG().getVertices().forEach(task -> {
      currentTaskGroupTaskIds.add(task.getId());
      idToTaskStates.put(task.getId(), new TaskState());
    });
  }


  /**
   * Updates the state of the task group.
   * @param newState of the task group.
   * @param failedTaskIds the ID of the task on which this task group failed if failed, empty otherwise.
   */
  public synchronized void onTaskGroupStateChanged(final TaskGroupState.State newState,
                                                   final Optional<List<String>> failedTaskIds) {
    switch (newState) {
    case EXECUTING:
      LOG.log(Level.FINE, "Executing TaskGroup ID {0}...", taskGroupId);
      idToTaskStates.forEach((taskId, state) -> state.getStateMachine().setState(TaskState.State.PENDING_IN_EXECUTOR));
      break;
    case COMPLETE:
      LOG.log(Level.FINE, "TaskGroup ID {0} complete!", taskGroupId);
      notifyTaskGroupStateToMaster(newState, failedTaskIds);
      break;
    case FAILED_RECOVERABLE:
      LOG.log(Level.FINE, "TaskGroup ID {0} failed (recoverable).", taskGroupId);
      notifyTaskGroupStateToMaster(newState, failedTaskIds);
      break;
    case FAILED_UNRECOVERABLE:
      LOG.log(Level.FINE, "TaskGroup ID {0} failed (unrecoverable).", taskGroupId);
      notifyTaskGroupStateToMaster(newState, failedTaskIds);
      break;
    default:
      throw new IllegalStateException("Illegal state at this point");
    }
  }

  /**
   * Updates the state of a task.
   * Task state changes only occur in executor.
   * @param taskId of the task.
   * @param newState of the task.
   */
  public synchronized void onTaskStateChanged(final String taskId, final TaskState.State newState) {
    final StateMachine taskStateChanged = idToTaskStates.get(taskId).getStateMachine();
    LOG.log(Level.FINE, "Task State Transition: id {0} from {1} to {2}",
        new Object[]{taskGroupId, taskStateChanged.getCurrentState(), newState});
    taskStateChanged.setState(newState);
    switch (newState) {
    case READY:
    case EXECUTING:
      break;
    case COMPLETE:
      currentTaskGroupTaskIds.remove(taskId);
      if (currentTaskGroupTaskIds.isEmpty()) {
        onTaskGroupStateChanged(TaskGroupState.State.COMPLETE, Optional.empty());
      }
      break;
    case FAILED_RECOVERABLE:
      onTaskGroupStateChanged(TaskGroupState.State.FAILED_RECOVERABLE, Optional.of(Arrays.asList(taskId)));
      break;
    case FAILED_UNRECOVERABLE:
      onTaskGroupStateChanged(TaskGroupState.State.FAILED_UNRECOVERABLE, Optional.of(Arrays.asList(taskId)));
      break;
    default:
      throw new IllegalStateException("Illegal state at this point");
    }
  }

  /**
   * Notifies the change in task group state to master.
   * @param newState of the task group.
   * @param failedTaskIds the id of the task that caused this task group to fail, empty otherwise.
   */
  private void notifyTaskGroupStateToMaster(final TaskGroupState.State newState,
                                            final Optional<List<String>> failedTaskIds) {
    final Optional<List<String>> failedTaskIdList;
    if (!failedTaskIds.isPresent()) {
      failedTaskIdList = Optional.of(Collections.emptyList());
    } else {
      failedTaskIdList = failedTaskIds;
    }

    // Send taskGroupStateChangedMsg to master!
    persistentConnectionToMaster.getMessageSender().send(
        ControlMessage.Message.newBuilder()
            .setId(RuntimeIdGenerator.generateMessageId())
            .setType(ControlMessage.MessageType.TaskGroupStateChanged)
            .setTaskStateChangedMsg(
                ControlMessage.TaskGroupStateChangedMsg.newBuilder()
                    .setExecutorId(executorId)
                    .setTaskGroupId(taskGroupId)
                    .setState(convertState(newState))
                    .addAllFailedTaskIds(failedTaskIdList.get())
                    .build())
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
    default:
      throw new UnknownExecutionStateException(new Exception("This TaskGroupState is unknown: " + state));
    }
  }

  // Tentative
  public void getCurrentTaskGroupExecutionState() {

  }
}
