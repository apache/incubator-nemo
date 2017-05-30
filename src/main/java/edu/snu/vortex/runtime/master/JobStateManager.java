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
package edu.snu.vortex.runtime.master;

import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.plan.RuntimeEdge;
import edu.snu.vortex.runtime.common.plan.physical.*;
import edu.snu.vortex.runtime.common.state.JobState;
import edu.snu.vortex.runtime.common.state.StageState;
import edu.snu.vortex.runtime.common.state.TaskGroupState;
import edu.snu.vortex.runtime.common.state.TaskState;
import edu.snu.vortex.runtime.exception.IllegalStateTransitionException;
import edu.snu.vortex.utils.StateMachine;
import edu.snu.vortex.utils.dag.DAG;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Manages the states related to a job.
 * This class can be used to track a job's execution status to task level in the future.
 * The methods of this class are synchronized.
 */
public final class JobStateManager {
  private static final Logger LOG = Logger.getLogger(JobStateManager.class.getName());

  private final String jobId;

  /**
   * The data structures below track the execution states of this job.
   */
  private final JobState jobState;
  private final Map<String, StageState> idToStageStates;
  private final Map<String, TaskGroupState> idToTaskGroupStates;
  private final Map<String, TaskState> idToTaskStates;

  /**
   * Represents the job to manage.
   */
  private final PhysicalPlan physicalPlan;

  /**
   * Used to track stage completion status.
   * All task group ids are added to the set when the a stage begins executing.
   * Each task group id is removed upon completion,
   * therefore indicating the stage's completion when this set becomes empty.
   */
  private final Map<String, Set<String>> stageIdToRemainingTaskGroupSet;

  /**
   * Used to track job completion status.
   * All stage ids are added to the set when the this job begins executing.
   * Each stage id is removed upon completion,
   * therefore indicating the job's completion when this set becomes empty.
   */
  private final Set<String> currentJobStageIds;

  public JobStateManager(final PhysicalPlan physicalPlan,
                         final BlockManagerMaster blockManagerMaster) {
    this.physicalPlan = physicalPlan;
    this.jobId = physicalPlan.getId();
    this.jobState = new JobState();
    this.idToStageStates = new HashMap<>();
    this.idToTaskGroupStates = new HashMap<>();
    this.idToTaskStates = new HashMap<>();
    this.stageIdToRemainingTaskGroupSet = new HashMap<>();
    this.currentJobStageIds = new HashSet<>();
    initializeComputationStates();
    initializeBlockStates(blockManagerMaster);
  }

  /**
   * Initializes the states for the job/stages/taskgroups/tasks for this job.
   */
  private void initializeComputationStates() {
    onJobStateChanged(JobState.State.EXECUTING);

    // Initialize the states for the job down to task-level.
    physicalPlan.getStageDAG().topologicalDo(physicalStage -> {
      currentJobStageIds.add(physicalStage.getId());
      idToStageStates.put(physicalStage.getId(), new StageState());
      physicalStage.getTaskGroupList().forEach(taskGroup -> {
        idToTaskGroupStates.put(taskGroup.getTaskGroupId(), new TaskGroupState());
        taskGroup.getTaskDAG().getVertices().forEach(
            task -> idToTaskStates.put(task.getId(), new TaskState()));
      });
    });
  }

  private void initializeBlockStates(final BlockManagerMaster blockManagerMaster) {
    final DAG<PhysicalStage, PhysicalStageEdge> stageDAG = physicalPlan.getStageDAG();
    stageDAG.topologicalDo(physicalStage -> {
      final List<TaskGroup> taskGroupsForStage = physicalStage.getTaskGroupList();
      final List<PhysicalStageEdge> stageOutgoingEdges = stageDAG.getOutgoingEdgesOf(physicalStage);

      // Initialize states for blocks of inter-stage edges
      stageOutgoingEdges.forEach(physicalStageEdge -> {
        final RuntimeAttribute commPattern =
            physicalStageEdge.getEdgeAttributes().get(RuntimeAttribute.Key.CommPattern);
        final int srcParallelism = taskGroupsForStage.size();
        IntStream.range(0, srcParallelism).forEach(srcTaskIdx -> {
          if (commPattern == RuntimeAttribute.ScatterGather) {
            final int dstParallelism =
                physicalStageEdge.getExternalVertexAttr().get(RuntimeAttribute.IntegerKey.Parallelism);
            IntStream.range(0, dstParallelism).forEach(dstTaskIdx ->
                blockManagerMaster.initializeState(physicalStageEdge.getId(), srcTaskIdx, dstTaskIdx));
          } else {
            blockManagerMaster.initializeState(physicalStageEdge.getId(), srcTaskIdx);
          }
        });
      });

      // Initialize states for blocks of stage internal edges
      taskGroupsForStage.forEach(taskGroup -> {
        final DAG<Task, RuntimeEdge<Task>> taskGroupInternalDag = taskGroup.getTaskDAG();
        taskGroupInternalDag.getVertices().forEach(task -> {
          final List<RuntimeEdge<Task>> internalOutgoingEdges = taskGroupInternalDag.getOutgoingEdgesOf(task);
          internalOutgoingEdges.forEach(taskRuntimeEdge ->
              blockManagerMaster.initializeState(taskRuntimeEdge.getId(), taskGroup.getTaskGroupIdx()));
        });
      });
    });
  }

  /**
   * Updates the state of the job.
   * @param newState of the job.
   */
  public synchronized void onJobStateChanged(final JobState.State newState) {
    if (newState == JobState.State.EXECUTING) {
      LOG.log(Level.FINE, "Executing Job ID {0}...", jobId);
      jobState.getStateMachine().setState(newState);
    } else if (newState == JobState.State.COMPLETE) {
      LOG.log(Level.FINE, "Job ID {0} complete!", jobId);
      jobState.getStateMachine().setState(newState);
      dumpPreviousJobExecutionStateToFile();
    } else if (newState == JobState.State.FAILED) {
      LOG.log(Level.FINE, "Job ID {0} failed.", jobId);
      jobState.getStateMachine().setState(newState);
      dumpPreviousJobExecutionStateToFile();
    } else {
      throw new IllegalStateTransitionException(new Exception("Illegal Job State Transition"));
    }
  }

  /**
   * Updates the state of a stage.
   * Stage state changes only occur in master.
   * @param stageId of the stage.
   * @param newState of the stage.
   */
  public synchronized void onStageStateChanged(final String stageId, final StageState.State newState) {
    final StateMachine stageStateMachine = idToStageStates.get(stageId).getStateMachine();
    LOG.log(Level.FINE, "Stage State Transition: id {0} from {1} to {2}",
        new Object[]{stageId, stageStateMachine.getCurrentState(), newState});
    stageStateMachine.setState(newState);
    if (newState == StageState.State.EXECUTING) {
      for (final PhysicalStage stage : physicalPlan.getStageDAG().getVertices()) {
        if (stage.getId().equals(stageId)) {
          Set<String> remainingTaskGroupIds = new HashSet<>();
          remainingTaskGroupIds.addAll(
              stage.getTaskGroupList()
                  .stream()
                  .map(taskGroup -> taskGroup.getTaskGroupId())
                  .collect(Collectors.toSet()));
          stageIdToRemainingTaskGroupSet.put(stageId, remainingTaskGroupIds);
          break;
        }
      }
    } else if (newState == StageState.State.COMPLETE) {
      currentJobStageIds.remove(stageId);
      if (currentJobStageIds.isEmpty()) {
        onJobStateChanged(JobState.State.COMPLETE);
      }
    }
  }

  /**
   * Updates the state of a task group.
   * Task group state changes can occur both in master and executor.
   * State changes that occur in master are initiated in {@link edu.snu.vortex.runtime.master.scheduler.BatchScheduler}.
   * State changes that occur in executors are sent to master as a control message,
   * and the call to this method is initiated in {@link edu.snu.vortex.runtime.master.scheduler.BatchScheduler}
   * when the message/event is received.
   * A task group completion implies completion of all its tasks.
   * @param taskGroupId of the task group.
   * @param newState of the task group.
   */
  public synchronized void onTaskGroupStateChanged(final String taskGroupId, final TaskGroupState.State newState) {
    final StateMachine taskGroupStateChanged = idToTaskGroupStates.get(taskGroupId).getStateMachine();
    LOG.log(Level.FINE, "Task Group State Transition: id {0} from {1} to {2}",
        new Object[]{taskGroupId, taskGroupStateChanged.getCurrentState(), newState});
    taskGroupStateChanged.setState(newState);
    if (newState == TaskGroupState.State.COMPLETE) {
      final TaskGroup taskGroup = getTaskGroupById(taskGroupId);
      final String stageId = taskGroup.getStageId();
      stageIdToRemainingTaskGroupSet.get(stageId).remove(taskGroupId);
      // TODO #235: Cleanup Task State Management
      taskGroup.getTaskDAG().getVertices().forEach(task -> {
        idToTaskStates.get(task.getId()).getStateMachine().setState(TaskState.State.PENDING_IN_EXECUTOR);
        idToTaskStates.get(task.getId()).getStateMachine().setState(TaskState.State.EXECUTING);
        idToTaskStates.get(task.getId()).getStateMachine().setState(TaskState.State.COMPLETE);
      });

      if (stageIdToRemainingTaskGroupSet.containsKey(stageId)) {
        if (stageIdToRemainingTaskGroupSet.get(stageId).isEmpty()) {
          onStageStateChanged(stageId, StageState.State.COMPLETE);
        }
      } else {
        throw new IllegalStateTransitionException(
            new Throwable("The stage has not yet been submitted for execution"));
      }
    }
  }

  private TaskGroup getTaskGroupById(final String taskGroupId) {
    for (final PhysicalStage physicalStage : physicalPlan.getStageDAG().getVertices()) {
      for (final TaskGroup taskGroup : physicalStage.getTaskGroupList()) {
        if (taskGroup.getTaskGroupId().equals(taskGroupId)) {
          return taskGroup;
        }
      }
    }
    throw new RuntimeException(new Throwable("This taskGroupId does not exist in the plan"));
  }

  public synchronized Optional<String> checkStageCompletion(final String taskGroupId) {
    final TaskGroup taskGroup = getTaskGroupById(taskGroupId);
    if (stageIdToRemainingTaskGroupSet.get(taskGroup.getStageId()).isEmpty()) {
      return Optional.of(taskGroup.getStageId());
    } else {
      return Optional.empty();
    }
  }

  public synchronized boolean checkJobCompletion() {
    return (jobState.getStateMachine().getCurrentState() == JobState.State.COMPLETE);
  }

  public synchronized String getJobId() {
    return jobId;
  }

  public synchronized StageState getStageState(final String stageId) {
    return idToStageStates.get(stageId);
  }

  public synchronized Map<String, StageState> getIdToStageStates() {
    return idToStageStates;
  }

  public synchronized Map<String, TaskGroupState> getIdToTaskGroupStates() {
    return idToTaskGroupStates;
  }

  public synchronized Map<String, TaskState> getIdToTaskStates() {
    return idToTaskStates;
  }

  // Tentative
  public void printCurrentJobExecutionState() {
    final StringBuffer sb = new StringBuffer("Job ID ");
    sb.append(this.jobId).append(":").append(jobState.getStateMachine().getCurrentState());
    sb.append("\n{Stages:\n{");
    physicalPlan.getStageDAG().topologicalDo(physicalStage -> {
      final StageState stageState = idToStageStates.get(physicalStage.getId());
      sb.append(physicalStage.getId()).append(":").append(stageState).append("\nTaskGroups:\n{");
      physicalStage.getTaskGroupList().forEach(taskGroup -> {
        sb.append(taskGroup.getTaskGroupId()).append(":").append(idToTaskGroupStates.get(taskGroup.getTaskGroupId()))
            .append(", Tasks:{\n");
        taskGroup.getTaskDAG().topologicalDo(
            task -> sb.append(task.getId()).append(":").append(idToTaskStates.get(task.getId())).append(","));
        sb.append("},\n");
      });
      sb.append("}}\n");
    });
    LOG.log(Level.INFO, sb.toString());
  }

  // Tentative
  private void dumpPreviousJobExecutionStateToFile() {

  }
}
