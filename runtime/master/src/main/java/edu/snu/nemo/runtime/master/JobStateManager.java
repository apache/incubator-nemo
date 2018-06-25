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
package edu.snu.nemo.runtime.master;

import edu.snu.nemo.common.exception.IllegalStateTransitionException;
import edu.snu.nemo.common.exception.SchedulingException;
import edu.snu.nemo.common.exception.UnknownExecutionStateException;
import edu.snu.nemo.common.StateMachine;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.runtime.common.metric.MetricDataBuilder;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.plan.PhysicalPlan;
import edu.snu.nemo.runtime.common.plan.Stage;
import edu.snu.nemo.runtime.common.plan.StageEdge;
import edu.snu.nemo.runtime.common.plan.RuntimeEdge;
import edu.snu.nemo.runtime.common.state.JobState;
import edu.snu.nemo.runtime.common.state.StageState;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import edu.snu.nemo.runtime.common.state.TaskState;
import org.apache.reef.annotations.audience.DriverSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.stream.IntStream;

import static edu.snu.nemo.common.dag.DAG.EMPTY_DAG_DIRECTORY;

/**
 * Maintains three levels of state machines (JobState, StageState, and TaskState) of a physical plan.
 * The main API this class provides is onTaskStateReportFromExecutor(), which directly changes a TaskState.
 * JobState and StageState are updated internally in the class, and can only be read from the outside.
 *
 * (CONCURRENCY) The public methods of this class are synchronized.
 */
@DriverSide
@ThreadSafe
public final class JobStateManager {
  private static final Logger LOG = LoggerFactory.getLogger(JobStateManager.class.getName());
  private final String jobId;
  private final int maxScheduleAttempt;

  /**
   * The data structures below track the execution states of this job.
   */
  private final JobState jobState;
  private final Map<String, StageState> idToStageStates;
  private final Map<String, TaskState> idToTaskStates;

  /**
   * Maintain the number of schedule attempts for each task.
   * The attempt numbers are updated only here, and are read-only in other places.
   */
  private final Map<String, Integer> taskIdToCurrentAttempt;

  /**
   * Represents the job to manage.
   */
  private final PhysicalPlan physicalPlan;

  /**
   * A lock and condition to check whether the job is finished or not.
   */
  private final Lock finishLock;
  private final Condition jobFinishedCondition;

  /**
   * For metrics.
   */
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
    this.idToTaskStates = new HashMap<>();
    this.taskIdToCurrentAttempt = new HashMap<>();
    this.finishLock = new ReentrantLock();
    this.jobFinishedCondition = finishLock.newCondition();
    this.metricDataBuilderMap = new HashMap<>();
    initializeComputationStates();
    initializePartitionStates(blockManagerMaster);
  }

  /**
   * Initializes the states for the job/stages/tasks for this job.
   */
  private void initializeComputationStates() {
    onJobStateChanged(JobState.State.EXECUTING);

    // Initialize the states for the job down to task-level.
    physicalPlan.getStageDAG().topologicalDo(stage -> {
      idToStageStates.put(stage.getId(), new StageState());
      stage.getTaskIds().forEach(taskId -> {
        idToTaskStates.put(taskId, new TaskState());
        taskIdToCurrentAttempt.put(taskId, 1);
      });
    });
  }

  private void initializePartitionStates(final BlockManagerMaster blockManagerMaster) {
    final DAG<Stage, StageEdge> stageDAG = physicalPlan.getStageDAG();
    stageDAG.topologicalDo(stage -> {
      final List<String> taskIdsForStage = stage.getTaskIds();
      final List<StageEdge> stageOutgoingEdges = stageDAG.getOutgoingEdgesOf(stage);

      // Initialize states for blocks of inter-stage edges
      stageOutgoingEdges.forEach(stageEdge -> {
        final int srcParallelism = taskIdsForStage.size();
        IntStream.range(0, srcParallelism).forEach(srcTaskIdx -> {
          final String blockId = RuntimeIdGenerator.generateBlockId(stageEdge.getId(), srcTaskIdx);
          blockManagerMaster.initializeState(blockId, taskIdsForStage.get(srcTaskIdx));
        });
      });

      // Initialize states for blocks of stage internal edges
      taskIdsForStage.forEach(taskId -> {
        final DAG<IRVertex, RuntimeEdge<IRVertex>> taskInternalDag = stage.getIRDAG();
        taskInternalDag.getVertices().forEach(task -> {
          final List<RuntimeEdge<IRVertex>> internalOutgoingEdges = taskInternalDag.getOutgoingEdgesOf(task);
          internalOutgoingEdges.forEach(taskRuntimeEdge -> {
            final int srcTaskIdx = RuntimeIdGenerator.getIndexFromTaskId(taskId);
            final String blockId = RuntimeIdGenerator.generateBlockId(taskRuntimeEdge.getId(), srcTaskIdx);
            blockManagerMaster.initializeState(blockId, taskId);
          });
        });
      });
    });
  }

  /**
   * Updates the state of a task.
   * Task state changes can occur both in master and executor.
   * State changes that occur in master are
   * initiated in {@link edu.snu.nemo.runtime.master.scheduler.BatchSingleJobScheduler}.
   * State changes that occur in executors are sent to master as a control message,
   * and the call to this method is initiated in {@link edu.snu.nemo.runtime.master.scheduler.BatchSingleJobScheduler}
   * when the message/event is received.
   *
   * @param taskId  the ID of the task.
   * @param newTaskState     the new state of the task.
   */
  public synchronized void onTaskStateChanged(final String taskId, final TaskState.State newTaskState) {
    // Change task state
    final StateMachine taskState = idToTaskStates.get(taskId).getStateMachine();
    LOG.debug("Task State Transition: id {}, from {} to {}",
        new Object[]{taskId, taskState.getCurrentState(), newTaskState});

    taskState.setState(newTaskState);

    // Handle metrics
    final Map<String, Object> metric = new HashMap<>();
    switch (newTaskState) {
      case ON_HOLD:
      case COMPLETE:
      case FAILED_UNRECOVERABLE:
      case FAILED_RECOVERABLE:
        metric.put("ToState", newTaskState);
        endMeasurement(taskId, metric);
        break;
      case EXECUTING:
        metric.put("FromState", newTaskState);
        beginMeasurement(taskId, metric);
        break;
      case READY:
        final int currentAttempt = taskIdToCurrentAttempt.get(taskId) + 1;
        metric.put("ScheduleAttempt", currentAttempt);
        if (currentAttempt <= maxScheduleAttempt) {
          taskIdToCurrentAttempt.put(taskId, currentAttempt);
        } else {
          throw new SchedulingException(new Throwable("Exceeded max number of scheduling attempts for " + taskId));
        }
        break;
      default:
        throw new UnknownExecutionStateException(new Throwable("This task state is unknown"));
    }

    // Change stage state, if needed
    final String stageId = RuntimeIdGenerator.getStageIdFromTaskId(taskId);
    final List<String> tasksOfThisStage = physicalPlan.getStageDAG().getVertexById(stageId).getTaskIds();
    final long numOfCompletedOrOnHoldTasksInThisStage = tasksOfThisStage
        .stream()
        .map(this::getTaskState)
        .filter(state -> state.equals(TaskState.State.COMPLETE) || state.equals(TaskState.State.ON_HOLD))
        .count();
    switch (newTaskState) {
      case READY:
        onStageStateChanged(stageId, StageState.State.READY);
        break;
      case EXECUTING:
        onStageStateChanged(stageId, StageState.State.EXECUTING);
        break;
      case FAILED_RECOVERABLE:
        onStageStateChanged(stageId, StageState.State.FAILED_RECOVERABLE);
        break;
      case COMPLETE:
      case ON_HOLD:
        if (numOfCompletedOrOnHoldTasksInThisStage == tasksOfThisStage.size()) {
          onStageStateChanged(stageId, StageState.State.COMPLETE);
        }
        break;
      case FAILED_UNRECOVERABLE:
        break;
      default:
        throw new UnknownExecutionStateException(new Throwable("This task state is unknown"));
    }

    // Log not-yet-completed tasks for us to track progress
    if (newTaskState.equals(TaskState.State.COMPLETE)) {
      LOG.info("{}: {} Task(s) to go", stageId, tasksOfThisStage.size() - numOfCompletedOrOnHoldTasksInThisStage);
    }
  }

  /**
   * (PRIVATE METHOD)
   * Updates the state of a stage.
   * @param stageId of the stage.
   * @param newStageState of the stage.
   */
  private void onStageStateChanged(final String stageId, final StageState.State newStageState) {
    if (newStageState.equals(getStageState(stageId))) {
      // Ignore duplicate state updates
      return;
    }

    // Change stage state
    final StateMachine stageStateMachine = idToStageStates.get(stageId).getStateMachine();
    LOG.debug("Stage State Transition: id {} from {} to {}",
        new Object[]{stageId, stageStateMachine.getCurrentState(), newStageState});
    stageStateMachine.setState(newStageState);

    // Metric handling
    final Map<String, Object> metric = new HashMap<>();
    if (newStageState == StageState.State.EXECUTING) {
      metric.put("FromState", newStageState);
      beginMeasurement(stageId, metric);
    } else if (newStageState == StageState.State.COMPLETE) {
      metric.put("ToState", newStageState);
      endMeasurement(stageId, metric);
    }

    // Change job state if needed
    final boolean allStagesCompleted = idToStageStates.values().stream().allMatch(state ->
        state.getStateMachine().getCurrentState().equals(StageState.State.COMPLETE));

    // (1) Job becomes EXECUTING if not already
    if (newStageState.equals(StageState.State.EXECUTING)
        && !getJobState().equals(JobState.State.EXECUTING)) {
      onJobStateChanged(JobState.State.EXECUTING);
    }
    // (2) Job becomes COMPLETE
    if (allStagesCompleted) {
      onJobStateChanged(JobState.State.COMPLETE);
    }
  }

  /**
   * (PRIVATE METHOD)
   * Updates the state of the job.
   * @param newState of the job.
   */
  private void onJobStateChanged(final JobState.State newState) {
    if (newState.equals(getJobState())) {
      // Ignore duplicate state updates
      return;
    }

    jobState.getStateMachine().setState(newState);

    final Map<String, Object> metric = new HashMap<>();
    if (newState == JobState.State.EXECUTING) {
      LOG.debug("Executing Job ID {}...", this.jobId);
      metric.put("FromState", newState);
      beginMeasurement(jobId, metric);
    } else if (newState == JobState.State.COMPLETE || newState == JobState.State.FAILED) {
      LOG.debug("Job ID {} {}!", new Object[]{jobId, newState});

      // Awake all threads waiting the finish of this job.
      finishLock.lock();
      metric.put("ToState", newState);
      endMeasurement(jobId, metric);

      try {
        jobFinishedCondition.signalAll();
      } finally {
        finishLock.unlock();
      }
    } else {
      throw new IllegalStateTransitionException(new Exception("Illegal Job State Transition"));
    }
  }


  /**
   * Wait for this job to be finished and return the final state.
   * @return the final state of this job.
   */
  public JobState.State waitUntilFinish() {
    finishLock.lock();
    try {
      if (!isJobDone()) {
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
  public JobState.State waitUntilFinish(final long timeout, final TimeUnit unit) {
    finishLock.lock();
    try {
      if (!isJobDone()) {
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

  public synchronized boolean isJobDone() {
    return (getJobState() == JobState.State.COMPLETE || getJobState() == JobState.State.FAILED);
  }
  public synchronized String getJobId() {
    return jobId;
  }

  public synchronized JobState.State getJobState() {
    return (JobState.State) jobState.getStateMachine().getCurrentState();
  }

  public synchronized StageState.State getStageState(final String stageId) {
    return (StageState.State) idToStageStates.get(stageId).getStateMachine().getCurrentState();
  }

  public synchronized TaskState.State getTaskState(final String taskId) {
    return (TaskState.State) idToTaskStates.get(taskId).getStateMachine().getCurrentState();
  }

  public synchronized int getTaskAttempt(final String taskId) {
    if (taskIdToCurrentAttempt.containsKey(taskId)) {
      return taskIdToCurrentAttempt.get(taskId);
    } else {
      throw new IllegalStateException("No mapping for this task's attemptIdx, an inconsistent state occurred.");
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

    // may be null when a Task fails without entering the executing state (due to an input read failure)
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
    sb.append("\"stages\": [");
    boolean isFirstStage = true;
    for (final Stage stage : physicalPlan.getStageDAG().getVertices()) {
      if (!isFirstStage) {
        sb.append(", ");
      }
      isFirstStage = false;
      final StageState stageState = idToStageStates.get(stage.getId());
      sb.append("{\"id\": \"").append(stage.getId()).append("\", ");
      sb.append("\"state\": \"").append(stageState.toString()).append("\", ");
      sb.append("\"tasks\": [");

      boolean isFirstTask = true;
      for (final String taskId : stage.getTaskIds()) {
        if (!isFirstTask) {
          sb.append(", ");
        }
        isFirstTask = false;
        final TaskState taskState = idToTaskStates.get(taskId);
        sb.append("{\"id\": \"").append(taskId).append("\", ");
        sb.append("\"state\": \"").append(taskState.toString()).append("\"}");
      }
      sb.append("]}");
    }
    sb.append("]}");
    return sb.toString();
  }
}
