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
package edu.snu.nemo.runtime.master;

import edu.snu.nemo.common.exception.IllegalStateTransitionException;
import edu.snu.nemo.common.exception.SchedulingException;
import edu.snu.nemo.common.exception.UnknownExecutionStateException;
import edu.snu.nemo.common.StateMachine;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.runtime.common.metric.MetricDataBuilder;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.plan.RuntimeEdge;
import edu.snu.nemo.runtime.common.plan.physical.*;
import edu.snu.nemo.runtime.common.state.JobState;
import edu.snu.nemo.runtime.common.state.StageState;
import edu.snu.nemo.runtime.common.state.TaskGroupState;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.reef.annotations.audience.DriverSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static edu.snu.nemo.common.dag.DAG.EMPTY_DAG_DIRECTORY;

/**
 * Manages the states related to a job.
 * This class can be used to track a job's execution status to task level in the future.
 * The methods of this class are synchronized.
 */
@DriverSide
public final class JobStateManager {
  private static final Logger LOG = LoggerFactory.getLogger(JobStateManager.class.getName());

  private final String jobId;

  private final int maxScheduleAttempt;

  /**
   * The data structures below track the execution states of this job.
   */
  private final JobState jobState;
  private final Map<String, StageState> idToStageStates;
  private final Map<String, TaskGroupState> idToTaskGroupStates;

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

  private final MetricMessageHandler metricMessageHandler;
  private final Map<String, MetricDataBuilder> metricDataBuilderMap;

  public JobStateManager(final PhysicalPlan physicalPlan,
                         final BlockManagerMaster blockManagerMaster,
                         final MetricMessageHandler metricMessageHandler,
                         final int maxScheduleAttempt) {
    this.jobId = physicalPlan.getId();
    this.physicalPlan = physicalPlan;
    this.metricMessageHandler = metricMessageHandler;
    this.maxScheduleAttempt = maxScheduleAttempt;
    this.jobState = new JobState();
    this.idToStageStates = new HashMap<>();
    this.idToTaskGroupStates = new HashMap<>();
    this.scheduleAttemptIdxByStage = new HashMap<>();
    this.stageIdToRemainingTaskGroupSet = new HashMap<>();
    this.currentJobStageIds = new HashSet<>();
    this.finishLock = new ReentrantLock();
    this.jobFinishedCondition = finishLock.newCondition();
    this.metricDataBuilderMap = new HashMap<>();
    initializeComputationStates();
    initializePartitionStates(blockManagerMaster);
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
      physicalStage.getTaskGroupIds().forEach(taskGroupId -> {
        idToTaskGroupStates.put(taskGroupId, new TaskGroupState());
      });
    });
  }

  private void initializePartitionStates(final BlockManagerMaster blockManagerMaster) {
    final DAG<PhysicalStage, PhysicalStageEdge> stageDAG = physicalPlan.getStageDAG();
    stageDAG.topologicalDo(physicalStage -> {
      final List<String> taskGroupIdsForStage = physicalStage.getTaskGroupIds();
      final List<PhysicalStageEdge> stageOutgoingEdges = stageDAG.getOutgoingEdgesOf(physicalStage);

      // Initialize states for blocks of inter-stage edges
      stageOutgoingEdges.forEach(physicalStageEdge -> {
        final int srcParallelism = taskGroupIdsForStage.size();
        IntStream.range(0, srcParallelism).forEach(srcTaskIdx -> {
          final String blockId = RuntimeIdGenerator.generateBlockId(physicalStageEdge.getId(), srcTaskIdx);
          blockManagerMaster.initializeState(blockId, taskGroupIdsForStage.get(srcTaskIdx));
        });
      });

      // Initialize states for blocks of stage internal edges
      taskGroupIdsForStage.forEach(taskGroupId -> {
        final DAG<Task, RuntimeEdge<Task>> taskGroupInternalDag = physicalStage.getTaskGroupDag();
        taskGroupInternalDag.getVertices().forEach(task -> {
          final List<RuntimeEdge<Task>> internalOutgoingEdges = taskGroupInternalDag.getOutgoingEdgesOf(task);
          internalOutgoingEdges.forEach(taskRuntimeEdge -> {
            final int srcTaskIdx = RuntimeIdGenerator.getIndexFromTaskGroupId(taskGroupId);
            final String blockId = RuntimeIdGenerator.generateBlockId(taskRuntimeEdge.getId(), srcTaskIdx);
            blockManagerMaster.initializeState(blockId, taskGroupId);
          });
        });
      });
    });
  }

  /**
   * Updates the state of the job.
   * @param newState of the job.
   */
  public synchronized void onJobStateChanged(final JobState.State newState) {
    final Map<String, Object> metric = new HashMap<>();

    if (newState == JobState.State.EXECUTING) {
      LOG.debug("Executing Job ID {}...", this.jobId);
      jobState.getStateMachine().setState(newState);
      metric.put("FromState", newState);
      beginMeasurement(jobId, metric);
    } else if (newState == JobState.State.COMPLETE || newState == JobState.State.FAILED) {
      LOG.debug("Job ID {} {}!", new Object[]{jobId, newState});
      // Awake all threads waiting the finish of this job.
      finishLock.lock();
      try {
        jobState.getStateMachine().setState(newState);
        metric.put("ToState", newState);
        endMeasurement(jobId, metric);

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
    LOG.debug("Stage State Transition: id {} from {} to {}",
        new Object[]{stageId, stageStateMachine.getCurrentState(), newState});
    stageStateMachine.setState(newState);
    final Map<String, Object> metric = new HashMap<>();

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

      metric.put("ScheduleAttempt", scheduleAttemptIdxByStage.get(stageId));
      metric.put("FromState", newState);
      beginMeasurement(stageId, metric);

      // if there exists a mapping, this state change is from a failed_recoverable stage,
      // and there may be task groups that do not need to be re-executed.
      if (!stageIdToRemainingTaskGroupSet.containsKey(stageId)) {
        for (final PhysicalStage stage : physicalPlan.getStageDAG().getVertices()) {
          if (stage.getId().equals(stageId)) {
            Set<String> remainingTaskGroupIds = new HashSet<>();
            remainingTaskGroupIds.addAll(
                stage.getTaskGroupIds().stream().collect(Collectors.toSet()));
            stageIdToRemainingTaskGroupSet.put(stageId, remainingTaskGroupIds);
            break;
          }
        }
      }
    } else if (newState == StageState.State.COMPLETE) {
      metric.put("ToState", newState);
      endMeasurement(stageId, metric);

      currentJobStageIds.remove(stageId);
      if (currentJobStageIds.isEmpty()) {
        onJobStateChanged(JobState.State.COMPLETE);
      }
    } else if (newState == StageState.State.FAILED_RECOVERABLE) {
      metric.put("ToState", newState);
      endMeasurement(stageId, metric);
      currentJobStageIds.add(stageId);
    } else if (newState == StageState.State.FAILED_UNRECOVERABLE) {
      metric.put("ToState", newState);
      endMeasurement(stageId, metric);
    }
  }

  /**
   * Updates the state of a task group.
   * Task group state changes can occur both in master and executor.
   * State changes that occur in master are
   * initiated in {@link edu.snu.nemo.runtime.master.scheduler.BatchSingleJobScheduler}.
   * State changes that occur in executors are sent to master as a control message,
   * and the call to this method is initiated in {@link edu.snu.nemo.runtime.master.scheduler.BatchSingleJobScheduler}
   * when the message/event is received.
   * A task group completion implies completion of all its tasks.
   *
   * @param taskGroupId  the ID of the task group.
   * @param newState     the new state of the task group.
   */
  public synchronized void onTaskGroupStateChanged(final String taskGroupId,
                                                   final TaskGroupState.State newState) {
    final StateMachine taskGroupState = idToTaskGroupStates.get(taskGroupId).getStateMachine();
    final String stageId = RuntimeIdGenerator.getStageIdFromTaskGroupId(taskGroupId);
    LOG.debug("Task Group State Transition: id {}, from {} to {}",
        new Object[]{taskGroupId, taskGroupState.getCurrentState(), newState});
    final Map<String, Object> metric = new HashMap<>();

    switch (newState) {
    case ON_HOLD:
    case COMPLETE:
      taskGroupState.setState(newState);
      metric.put("ToState", newState);
      endMeasurement(taskGroupId, metric);

      if (stageIdToRemainingTaskGroupSet.containsKey(stageId)) {
        final Set<String> remainingTaskGroups = stageIdToRemainingTaskGroupSet.get(stageId);
        LOG.info("{}: {} TaskGroup(s) to go", stageId, remainingTaskGroups.size());
        remainingTaskGroups.remove(taskGroupId);

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
      metric.put("FromState", newState);
      beginMeasurement(taskGroupId, metric);
      break;
    case FAILED_RECOVERABLE:
      // Multiple calls to set a task group's state to failed_recoverable can occur when
      // a task group is made failed_recoverable early by another task group's failure detection in the same stage
      // and the task group finds itself failed_recoverable later, propagating the state change event only then.
      if (taskGroupState.getCurrentState() != TaskGroupState.State.FAILED_RECOVERABLE) {
        taskGroupState.setState(newState);
        metric.put("ToState", newState);
        endMeasurement(taskGroupId, metric);

        // Mark this stage as failed_recoverable as long as it contains at least one failed_recoverable task group
        if (idToStageStates.get(stageId).getStateMachine().getCurrentState() != StageState.State.FAILED_RECOVERABLE) {
          onStageStateChanged(stageId, StageState.State.FAILED_RECOVERABLE);
        }

        if (stageIdToRemainingTaskGroupSet.containsKey(stageId)) {
          stageIdToRemainingTaskGroupSet.get(stageId).add(taskGroupId);
        } else {
          throw new IllegalStateTransitionException(
              new Throwable("The stage has not yet been submitted for execution"));
        }
      } else {
        LOG.info("{} state is already FAILED_RECOVERABLE. Skipping this event.",
            taskGroupId);
      }
      break;
    case READY:
      taskGroupState.setState(newState);
      break;
    case FAILED_UNRECOVERABLE:
      taskGroupState.setState(newState);
      metric.put("ToState", newState);
      endMeasurement(taskGroupId, metric);
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
      LOG.warn("Interrupted during waiting the finish of Job ID {}", jobId);
      Thread.currentThread().interrupt();
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
        if (!jobFinishedCondition.await(timeout, unit)) {
          LOG.warn("Timeout during waiting the finish of Job ID {}", jobId);
        }
      }
    } catch (final InterruptedException e) {
      LOG.warn("Interrupted during waiting the finish of Job ID {}", jobId);
      Thread.currentThread().interrupt();
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

    // may be null when a TaskGroup fails without entering the executing state (due to an input read failure)
    if (metricDataBuilder != null) {
      finalMetric.put("ContainerId", "Master");
      metricDataBuilder.endMeasurement(finalMetric);
      metricMessageHandler.onMetricMessageReceived(compUnitId, metricDataBuilder.build().toJson());
      metricDataBuilderMap.remove(compUnitId);
    }
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
    try (final PrintWriter printWriter = new PrintWriter(file)) {
      printWriter.println(toStringWithPhysicalPlan());
      LOG.debug(String.format("JSON representation of job state for %s(%s) was saved to %s",
          jobId, suffix, file.getPath()));
    } catch (final IOException e) {
      LOG.warn(String.format("Cannot store JSON representation of job state for %s(%s) to %s: %s",
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
      for (final String taskGroupId : stage.getTaskGroupIds()) {
        if (!isFirstTaskGroup) {
          sb.append(", ");
        }
        isFirstTaskGroup = false;
        final TaskGroupState taskGroupState = idToTaskGroupStates.get(taskGroupId);
        sb.append("{\"id\": \"").append(taskGroupId).append("\", ");
        sb.append("\"state\": \"").append(taskGroupState.toString()).append("\"}");
      }
      sb.append("]}");
    }
    sb.append("]}");
    return sb.toString();
  }
}
