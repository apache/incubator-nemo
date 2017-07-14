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

import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.runtime.common.plan.RuntimeEdge;
import edu.snu.vortex.runtime.common.plan.physical.*;
import edu.snu.vortex.runtime.common.state.JobState;
import edu.snu.vortex.runtime.common.state.StageState;
import edu.snu.vortex.runtime.common.state.TaskGroupState;
import edu.snu.vortex.runtime.common.state.TaskState;
import edu.snu.vortex.runtime.exception.IllegalStateTransitionException;
import edu.snu.vortex.runtime.exception.SchedulingException;
import edu.snu.vortex.common.StateMachine;
import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.runtime.exception.UnknownExecutionStateException;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static edu.snu.vortex.common.dag.DAG.EMPTY_DAG_DIRECTORY;

/**
 * Manages the states related to a job.
 * This class can be used to track a job's execution status to task level in the future.
 * The methods of this class are synchronized.
 */
public final class JobStateManager {
  private static final Logger LOG = Logger.getLogger(JobStateManager.class.getName());

  private final String jobId;

  private final int maxScheduleAttempt;

  /**
   * The data structures below track the execution states of this job.
   */
  private final JobState jobState;
  private final Map<String, StageState> idToStageStates;
  private final Map<String, TaskGroupState> idToTaskGroupStates;
  private final Map<String, TaskState> idToTaskStates;

  /**
   * Keeps track of the number of schedule attempts for each stage.
   */
  private final Map<String, Integer> scheduleAttemptIdxByStage;

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

  /**
   * A lock and condition to check whether the job is finished or not.
   */
  private final Lock finishLock;
  private final Condition jobFinishedCondition;

  public JobStateManager(final PhysicalPlan physicalPlan,
                         final PartitionManagerMaster partitionManagerMaster,
                         final int maxScheduleAttempt) {
    this.physicalPlan = physicalPlan;
    this.maxScheduleAttempt = maxScheduleAttempt;
    this.jobId = physicalPlan.getId();
    this.jobState = new JobState();
    this.idToStageStates = new HashMap<>();
    this.idToTaskGroupStates = new HashMap<>();
    this.idToTaskStates = new HashMap<>();
    this.scheduleAttemptIdxByStage = new HashMap<>();
    this.stageIdToRemainingTaskGroupSet = new HashMap<>();
    this.currentJobStageIds = new HashSet<>();
    this.finishLock = new ReentrantLock();
    this.jobFinishedCondition = finishLock.newCondition();
    initializeComputationStates();
    initializePartitionStates(partitionManagerMaster);
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

  private void initializePartitionStates(final PartitionManagerMaster partitionManagerMaster) {
    final DAG<PhysicalStage, PhysicalStageEdge> stageDAG = physicalPlan.getStageDAG();
    stageDAG.topologicalDo(physicalStage -> {
      final List<TaskGroup> taskGroupsForStage = physicalStage.getTaskGroupList();
      final List<PhysicalStageEdge> stageOutgoingEdges = stageDAG.getOutgoingEdgesOf(physicalStage);

      // Initialize states for partitions of inter-stage edges
      stageOutgoingEdges.forEach(physicalStageEdge -> {
        final Attribute commPattern =
            physicalStageEdge.getAttributes().get(Attribute.Key.CommunicationPattern);
        final int srcParallelism = taskGroupsForStage.size();
        IntStream.range(0, srcParallelism).forEach(srcTaskIdx -> {
          if (commPattern == Attribute.ScatterGather) {
            final int dstParallelism =
                physicalStageEdge.getExternalVertexAttr().get(Attribute.IntegerKey.Parallelism);
            IntStream.range(0, dstParallelism).forEach(dstTaskIdx ->
                partitionManagerMaster.initializeState(physicalStageEdge.getId(), srcTaskIdx, dstTaskIdx,
                    taskGroupsForStage.get(srcTaskIdx).getTaskGroupId()));
          } else {
            partitionManagerMaster.initializeState(physicalStageEdge.getId(), srcTaskIdx,
                taskGroupsForStage.get(srcTaskIdx).getTaskGroupId());
          }
        });
      });

      // Initialize states for partitions of stage internal edges
      taskGroupsForStage.forEach(taskGroup -> {
        final DAG<Task, RuntimeEdge<Task>> taskGroupInternalDag = taskGroup.getTaskDAG();
        taskGroupInternalDag.getVertices().forEach(task -> {
          final List<RuntimeEdge<Task>> internalOutgoingEdges = taskGroupInternalDag.getOutgoingEdgesOf(task);
          internalOutgoingEdges.forEach(taskRuntimeEdge ->
              partitionManagerMaster.initializeState(taskRuntimeEdge.getId(), taskGroup.getTaskGroupIdx(),
                  taskGroup.getTaskGroupId()));
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
      // Awake all threads waiting the finish of this job.
      finishLock.lock();
      try {
        jobState.getStateMachine().setState(newState);
        jobFinishedCondition.signalAll();
      } finally {
        finishLock.unlock();
      }
    } else if (newState == JobState.State.FAILED) {
      LOG.log(Level.FINE, "Job ID {0} failed.", jobId);
      // Awake all threads waiting the finish of this job.
      finishLock.lock();
      try {
        jobState.getStateMachine().setState(newState);
        jobFinishedCondition.signalAll();
      } finally {
        finishLock.unlock();
      }
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
      if (scheduleAttemptIdxByStage.containsKey(stageId)) {
        final int numAttempts = scheduleAttemptIdxByStage.get(stageId);

        if (numAttempts < maxScheduleAttempt) {
          scheduleAttemptIdxByStage.put(stageId, numAttempts + 1);
        } else {
          throw new SchedulingException(
              new Throwable("Exceeded max number of scheduling attempts for " + stageId));
        }
      } else {
        scheduleAttemptIdxByStage.put(stageId, 1);
      }

      // if there exists a mapping, this state change is from a failed_recoverable stage,
      // and there may be task groups that do not need to be re-executed.
      if (!stageIdToRemainingTaskGroupSet.containsKey(stageId)) {
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
      }
    } else if (newState == StageState.State.COMPLETE) {
      currentJobStageIds.remove(stageId);
      if (currentJobStageIds.isEmpty()) {
        onJobStateChanged(JobState.State.COMPLETE);
      }
    } else if (newState == StageState.State.FAILED_RECOVERABLE) {
      currentJobStageIds.add(stageId);
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
   * @param taskGroup the task group.
   * @param newState of the task group.
   */
  public synchronized void onTaskGroupStateChanged(final TaskGroup taskGroup, final TaskGroupState.State newState) {
    final StateMachine taskGroupState = idToTaskGroupStates.get(taskGroup.getTaskGroupId()).getStateMachine();
    LOG.log(Level.FINE, "Task Group State Transition: id {0} from {1} to {2}",
        new Object[]{taskGroup.getTaskGroupId(), taskGroupState.getCurrentState(), newState});
    final String stageId = taskGroup.getStageId();

    switch (newState) {
    case COMPLETE:
      taskGroupState.setState(newState);
      // TODO #235: Cleanup Task State Management
      taskGroup.getTaskDAG().getVertices().forEach(task -> {
        idToTaskStates.get(task.getId()).getStateMachine().setState(TaskState.State.COMPLETE);
      });

      if (stageIdToRemainingTaskGroupSet.containsKey(stageId)) {
        final Set<String> remainingTaskGroups = stageIdToRemainingTaskGroupSet.get(stageId);
        remainingTaskGroups.remove(taskGroup.getTaskGroupId());

        if (remainingTaskGroups.isEmpty()) {
          onStageStateChanged(stageId, StageState.State.COMPLETE);
        }
      } else {
        throw new IllegalStateTransitionException(
            new Throwable("The stage has not yet been submitted for execution"));
      }
      break;
    case EXECUTING:
      taskGroupState.setState(newState);
      // TODO #235: Cleanup Task State Management
      taskGroup.getTaskDAG().getVertices().forEach(task -> {
        idToTaskStates.get(task.getId()).getStateMachine().setState(TaskState.State.PENDING_IN_EXECUTOR);
        idToTaskStates.get(task.getId()).getStateMachine().setState(TaskState.State.EXECUTING);
      });
      break;
    case FAILED_RECOVERABLE:
      // Multiple calls to set a task group's state to failed_recoverable can occur when
      // a task group is made failed_recoverable early by another task group's failure detection in the same stage
      // and the task group finds itself failed_recoverable later, propagating the state change event only then.
      if (taskGroupState.getCurrentState() != TaskGroupState.State.FAILED_RECOVERABLE) {
        taskGroupState.setState(newState);
        taskGroup.getTaskDAG().getVertices().forEach(task ->
            idToTaskStates.get(task.getId()).getStateMachine().setState(TaskState.State.FAILED_RECOVERABLE));

        // Mark this stage as failed_recoverable as long as it contains at least one failed_recoverable task group
        if (idToStageStates.get(stageId).getStateMachine().getCurrentState() != StageState.State.FAILED_RECOVERABLE) {
          onStageStateChanged(stageId, StageState.State.FAILED_RECOVERABLE);
        }

        if (stageIdToRemainingTaskGroupSet.containsKey(stageId)) {
          stageIdToRemainingTaskGroupSet.get(stageId).add(taskGroup.getTaskGroupId());
        } else {
          throw new IllegalStateTransitionException(
              new Throwable("The stage has not yet been submitted for execution"));
        }
      } else {
        LOG.log(Level.INFO, "{0} state is already FAILED_RECOVERABLE. Skipping this event.",
            taskGroup.getTaskGroupId());
      }
      break;
    case READY:
      // TODO #235: Cleanup Task State Management
      taskGroup.getTaskDAG().getVertices().forEach(task ->
          idToTaskStates.get(task.getId()).getStateMachine().setState(TaskState.State.READY));
      taskGroupState.setState(newState);
      break;
    case FAILED_UNRECOVERABLE:
      taskGroupState.setState(newState);
      break;
    default:
      throw new UnknownExecutionStateException(new Throwable("This task group state is unknown"));
    }
  }

  public synchronized boolean checkStageCompletion(final String stageId) {
    return stageIdToRemainingTaskGroupSet.get(stageId).isEmpty();
  }

  public synchronized boolean checkJobTermination() {
    final Enum currentState = jobState.getStateMachine().getCurrentState();
    return (currentState == JobState.State.COMPLETE || currentState == JobState.State.FAILED);
  }

  public synchronized int getAttemptCountForStage(final String stageId) {
    if (scheduleAttemptIdxByStage.containsKey(stageId)) {
      return scheduleAttemptIdxByStage.get(stageId);
    } else {
      throw new IllegalStateException("No mapping for this stage's attemptIdx, an inconsistent state occurred.");
    }
  }

  /**
   * Wait for this job to be finished and return the final state.
   * @return the final state of this job.
   */
  public JobState waitUntilFinish() {
    finishLock.lock();
    try {
      if (!checkJobTermination()) {
        jobFinishedCondition.await();
      }
    } catch (final InterruptedException e) {
      LOG.log(Level.WARNING, "Interrupted during waiting the finish of Job ID {0}", jobId);
    } finally {
      finishLock.unlock();
    }
    return getJobState();
  }

  /**
   * Wait for this job to be finished and return the final state.
   * It wait for at most the given time.
   * @param timeout of waiting.
   * @param unit of the timeout.
   * @return the final state of this job.
   */
  public JobState waitUntilFinish(final long timeout,
                                  final TimeUnit unit) {
    finishLock.lock();
    try {
      if (!checkJobTermination()) {
        jobFinishedCondition.await(timeout, unit);
      }
    } catch (final InterruptedException e) {
      LOG.log(Level.WARNING, "Interrupted during waiting the finish of Job ID {0}", jobId);
    } finally {
      finishLock.unlock();
    }
    return getJobState();
  }

  public synchronized String getJobId() {
    return jobId;
  }

  public synchronized JobState getJobState() {
    return jobState;
  }

  public synchronized StageState getStageState(final String stageId) {
    return idToStageStates.get(stageId);
  }

  public synchronized Map<String, StageState> getIdToStageStates() {
    return idToStageStates;
  }

  public synchronized TaskGroupState getTaskGroupState(final String taskGroupId) {
    return idToTaskGroupStates.get(taskGroupId);
  }

  public synchronized Map<String, TaskGroupState> getIdToTaskGroupStates() {
    return idToTaskGroupStates;
  }

  public synchronized Map<String, TaskState> getIdToTaskStates() {
    return idToTaskStates;
  }

  /**
   * Stores JSON representation of job state into a file.
   * @param directory the directory which JSON representation is saved to
   * @param suffix suffix for file name
   */
  public void storeJSON(final String directory, final String suffix) {
    if (directory.equals(EMPTY_DAG_DIRECTORY)) {
      return;
    }

    final File file = new File(directory, jobId + "-" + suffix + ".json");
    file.getParentFile().mkdirs();
    try {
      final PrintWriter printWriter = new PrintWriter(file);
      printWriter.println(toStringWithPhysicalPlan());
      printWriter.close();
      LOG.log(Level.INFO, String.format("JSON representation of job state for %s(%s) was saved to %s",
          jobId, suffix, file.getPath()));
    } catch (IOException e) {
      LOG.log(Level.WARNING, String.format("Cannot store JSON representation of job state for %s(%s) to %s: %s",
          jobId, suffix, file.getPath(), e.toString()));
    }
  }

  public String toStringWithPhysicalPlan() {
    final StringBuilder sb = new StringBuilder("{");
    sb.append("\"dag\": ").append(physicalPlan.getStageDAG().toString()).append(", ");
    sb.append("\"jobState\": ").append(toString()).append("}");
    return sb.toString();
  }

  @Override
  public synchronized String toString() {
    final StringBuilder sb = new StringBuilder("{");
    sb.append("\"jobId\": \"").append(jobId).append("\", ");
    sb.append("\"physicalStages\": [");
    boolean isFirstStage = true;
    for (final PhysicalStage stage : physicalPlan.getStageDAG().getVertices()) {
      if (!isFirstStage) {
        sb.append(", ");
      }
      isFirstStage = false;
      final StageState stageState = idToStageStates.get(stage.getId());
      sb.append("{\"id\": \"").append(stage.getId()).append("\", ");
      sb.append("\"state\": \"").append(stageState.toString()).append("\", ");
      sb.append("\"taskGroups\": [");

      boolean isFirstTaskGroup = true;
      for (final TaskGroup taskGroup : stage.getTaskGroupList()) {
        if (!isFirstTaskGroup) {
          sb.append(", ");
        }
        isFirstTaskGroup = false;
        final TaskGroupState taskGroupState = idToTaskGroupStates.get(taskGroup.getTaskGroupId());
        sb.append("{\"id\": \"").append(taskGroup.getTaskGroupId()).append("\", ");
        sb.append("\"state\": \"").append(taskGroupState.toString()).append("\", ");
        sb.append("\"tasks\": [");

        boolean isFirstTask = true;
        for (final Task task : taskGroup.getTaskDAG().getVertices()) {
          if (!isFirstTask) {
            sb.append(", ");
          }
          isFirstTask = false;
          final TaskState taskState = idToTaskStates.get(task.getId());
          sb.append("{\"id\": \"").append(task.getId()).append("\", ");
          sb.append("\"state\": \"").append(taskState.toString()).append("\"}");
        }
        sb.append("]}");
      }
      sb.append("]}");
    }
    sb.append("]}");
    return sb.toString();
  }
}
