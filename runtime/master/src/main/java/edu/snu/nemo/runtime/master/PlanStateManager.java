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

import com.google.common.annotations.VisibleForTesting;
import edu.snu.nemo.common.exception.IllegalStateTransitionException;
import edu.snu.nemo.common.exception.UnknownExecutionStateException;
import edu.snu.nemo.common.StateMachine;
import edu.snu.nemo.common.ir.vertex.executionproperty.ClonedSchedulingProperty;
import edu.snu.nemo.conf.JobConf;
import edu.snu.nemo.runtime.common.RuntimeIdManager;
import edu.snu.nemo.runtime.common.plan.PhysicalPlan;
import edu.snu.nemo.runtime.common.plan.Stage;
import edu.snu.nemo.runtime.common.state.PlanState;
import edu.snu.nemo.runtime.common.state.StageState;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import edu.snu.nemo.runtime.common.state.TaskState;
import edu.snu.nemo.runtime.common.metric.JobMetric;
import edu.snu.nemo.runtime.common.metric.StageMetric;
import edu.snu.nemo.runtime.common.metric.TaskMetric;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import static edu.snu.nemo.common.dag.DAG.EMPTY_DAG_DIRECTORY;

/**
 * Maintains three levels of state machines (PlanState, StageState, and TaskState) of a physical plan.
 * The main API this class provides is onTaskStateReportFromExecutor(), which directly changes a TaskState.
 * PlanState and StageState are updated internally in the class, and can only be read from the outside.
 *
 * (CONCURRENCY) The public methods of this class are synchronized.
 */
@DriverSide
@ThreadSafe
public final class PlanStateManager {
  private static final Logger LOG = LoggerFactory.getLogger(PlanStateManager.class.getName());
  private String planId;
  private int maxScheduleAttempt;
  private boolean initialized;
  private int dagLogFileIndex = 0;

  /**
   * The data structures below track the execution states of this plan.
   */
  private PlanState planState;
  private final Map<String, StageState> stageIdToState;
  private final Map<String, List<List<TaskState>>> stageIdToTaskAttemptStates; // sorted by task idx, and then attempt

  /**
   * Represents the plan to manage.
   */
  private PhysicalPlan physicalPlan;

  /**
   * A lock and condition to check whether the plan is finished or not.
   */
  private final Lock finishLock;
  private final Condition planFinishedCondition;

  /**
   * For metrics.
   */
  private final String dagDirectory;
  private final MetricMessageHandler metricMessageHandler;
  private MetricStore metricStore;

  /**
   * Constructor.
   *
   * @param metricMessageHandler the metric handler for the plan.
   */
  @Inject
  private PlanStateManager(@Parameter(JobConf.DAGDirectory.class) final String dagDirectory,
                           final MetricMessageHandler metricMessageHandler) {
    this.metricMessageHandler = metricMessageHandler;
    this.planState = new PlanState();
    this.stageIdToState = new HashMap<>();
    this.stageIdToTaskAttemptStates = new HashMap<>();
    this.finishLock = new ReentrantLock();
    this.planFinishedCondition = finishLock.newCondition();
    this.dagDirectory = dagDirectory;
    this.metricStore = MetricStore.getStore();
    this.initialized = false;
  }

  /**
   * Update the physical plan and maximum attempt.
   *
   * @param physicalPlanToUpdate    the physical plan to manage.
   * @param maxScheduleAttemptToSet the maximum number of times this plan/sub-part of the plan should be attempted.
   */
  public synchronized void updatePlan(final PhysicalPlan physicalPlanToUpdate,
                                      final int maxScheduleAttemptToSet) {
    if (!initialized) {
      // First scheduling.
      this.initialized = true;
    }
    this.planState = new PlanState();
    this.physicalPlan = physicalPlanToUpdate;
    this.planId = physicalPlanToUpdate.getPlanId();
    this.maxScheduleAttempt = maxScheduleAttemptToSet;
    this.metricStore.getOrCreateMetric(JobMetric.class, planId).setStageDAG(physicalPlanToUpdate.getStageDAG());
    this.metricStore.triggerBroadcast(JobMetric.class, planId);
    initializeComputationStates();
  }

  /**
   * Initializes the states for the plan/stages/tasks for this plan.
   * TODO #182: Consider reshaping in run-time optimization. At now, we only consider plan appending.
   */
  private void initializeComputationStates() {
    onPlanStateChanged(PlanState.State.EXECUTING);
    physicalPlan.getStageDAG().topologicalDo(stage -> {
      if (!stageIdToState.containsKey(stage.getId())) {
        stageIdToState.put(stage.getId(), new StageState());
        stageIdToTaskAttemptStates.put(stage.getId(), new ArrayList<>(stage.getParallelism()));
        // for each task idx of this stage
        for (int taskIndex = 0; taskIndex < stage.getParallelism(); taskIndex++) {
          // for each task idx of this stage
          stageIdToTaskAttemptStates.get(stage.getId()).add(new ArrayList<>());
          // task states will be initialized lazily in getTaskAttemptsToSchedule()
        }
      }
    });
  }

  /**
   * Get task attempts that are "READY".
   *
   * @param stageId to run
   * @return executable task attempts
   */
  public synchronized List<String> getTaskAttemptsToSchedule(final String stageId) {
    if (getStageState(stageId).equals(StageState.State.COMPLETE)) {
      // This stage is done
      return new ArrayList<>(0);
    }

    // For each task index....
    final List<String> taskAttemptsToSchedule = new ArrayList<>();
    final Stage stage = physicalPlan.getStageDAG().getVertexById(stageId);
    for (int taskIndex = 0; taskIndex < stage.getParallelism(); taskIndex++) {
      final List<TaskState> attemptStatesForThisTaskIndex =
        stageIdToTaskAttemptStates.get(stage.getId()).get(taskIndex);

      // If one of the attempts is COMPLETE, do not schedule
      if (attemptStatesForThisTaskIndex
        .stream()
        .noneMatch(state -> state.getStateMachine().getCurrentState().equals(TaskState.State.COMPLETE))) {

        // (Step 1) Create new READY attempts, as many as
        // # of clones - # of 'not-done' attempts)
        final int numOfClones = stage.getPropertyValue(ClonedSchedulingProperty.class).orElse(1);
        final long numOfNotDoneAttempts = attemptStatesForThisTaskIndex.stream().filter(this::isTaskNotDone).count();
        for (int i = 0; i < numOfClones - numOfNotDoneAttempts; i++) {
          attemptStatesForThisTaskIndex.add(new TaskState());
        }

        // (Step 2) Check max attempt
        if (attemptStatesForThisTaskIndex.size() > maxScheduleAttempt) {
          throw new RuntimeException(
            attemptStatesForThisTaskIndex.size() + " exceeds max attempt " + maxScheduleAttempt);
        }

        // (Step 3) Return all READY attempts
        for (int attempt = 0; attempt < attemptStatesForThisTaskIndex.size(); attempt++) {
          if (attemptStatesForThisTaskIndex.get(attempt).getStateMachine().getCurrentState()
            .equals(TaskState.State.READY)) {
            taskAttemptsToSchedule.add(RuntimeIdManager.generateTaskId(stageId, taskIndex, attempt));
          }
        }

      }
    }

    return taskAttemptsToSchedule;
  }

  private boolean isTaskNotDone(final TaskState taskState) {
    final TaskState.State state = (TaskState.State) taskState.getStateMachine().getCurrentState();
    return state.equals(TaskState.State.READY)
      || state.equals(TaskState.State.EXECUTING)
      || state.equals(TaskState.State.ON_HOLD);
  }

  /**
   * Gets the attempt numbers of all tasks in a stage.
   *
   * @param stageId the stage to investigate.
   * @return the attempt numbers of all tasks in a stage.
   */
  public synchronized Set<String> getAllTaskAttemptsOfStage(final String stageId) {
    return getTaskAttemptIdsToItsState(stageId).keySet();
  }

  /**
   * Updates the state of a task.
   * Task state changes can occur both in master and executor.
   * State changes that occur in master are
   * initiated in {@link edu.snu.nemo.runtime.master.scheduler.BatchScheduler}.
   * State changes that occur in executors are sent to master as a control message,
   * and the call to this method is initiated in {@link edu.snu.nemo.runtime.master.scheduler.BatchScheduler}
   * when the message/event is received.
   *
   * @param taskId       the ID of the task.
   * @param newTaskState the new state of the task.
   */
  public synchronized void onTaskStateChanged(final String taskId, final TaskState.State newTaskState) {
    // Change task state
    final StateMachine taskState = getTaskStateHelper(taskId).getStateMachine();
    LOG.debug("Task State Transition: id {}, from {} to {}",
      new Object[]{taskId, taskState.getCurrentState(), newTaskState});
    metricStore.getOrCreateMetric(TaskMetric.class, taskId)
      .addEvent((TaskState.State) taskState.getCurrentState(), newTaskState);
    metricStore.triggerBroadcast(TaskMetric.class, taskId);

    try {
      taskState.setState(newTaskState);
    } catch (IllegalStateTransitionException e) {
      throw new RuntimeException(taskId + " - Illegal task state transition ", e);
    }

    // Log not-yet-completed tasks for us humans to track progress
    final String stageId = RuntimeIdManager.getStageIdFromTaskId(taskId);
    final List<List<TaskState>> taskStatesOfThisStage = stageIdToTaskAttemptStates.get(stageId);
    final long numOfCompletedTaskIndicesInThisStage = taskStatesOfThisStage.stream()
      .map(attempts -> attempts.stream()
        .map(state -> state.getStateMachine().getCurrentState())
        .allMatch(curState -> curState.equals(TaskState.State.COMPLETE)
          || curState.equals(TaskState.State.SHOULD_RETRY)
          || curState.equals(TaskState.State.ON_HOLD)))
      .filter(bool -> bool.equals(true))
      .count();
    if (newTaskState.equals(TaskState.State.COMPLETE)) {
      LOG.info("{} completed: {} Task(s) out of {} are remaining in this stage",
        taskId, taskStatesOfThisStage.size() - numOfCompletedTaskIndicesInThisStage, taskStatesOfThisStage.size());
    }

    // Change stage state, if needed
    switch (newTaskState) {
      // INCOMPLETE stage
      case SHOULD_RETRY:
        final boolean isAPeerAttemptCompleted = getPeerAttemptsforTheSameTaskIndex(taskId).stream()
          .anyMatch(state -> state.equals(TaskState.State.COMPLETE));
        if (!isAPeerAttemptCompleted) {
          // None of the peers has completed, hence this stage is incomplete
          onStageStateChanged(stageId, StageState.State.INCOMPLETE);
        }
        break;

      // COMPLETE stage
      case COMPLETE:
      case ON_HOLD:
        if (numOfCompletedTaskIndicesInThisStage
          == physicalPlan.getStageDAG().getVertexById(stageId).getParallelism()) {
          onStageStateChanged(stageId, StageState.State.COMPLETE);
        }
        break;

      // Doesn't affect StageState
      case READY:
      case EXECUTING:
      case FAILED:
        break;
      default:
        throw new UnknownExecutionStateException(new Throwable("This task state is unknown"));
    }
  }

  private List<TaskState.State> getPeerAttemptsforTheSameTaskIndex(final String taskId) {
    final String stageId = RuntimeIdManager.getStageIdFromTaskId(taskId);
    final int taskIndex = RuntimeIdManager.getIndexFromTaskId(taskId);
    final int attempt = RuntimeIdManager.getAttemptFromTaskId(taskId);

    final List<TaskState> otherAttemptsforTheSameTaskIndex =
      new ArrayList<>(stageIdToTaskAttemptStates.get(stageId).get(taskIndex));
    otherAttemptsforTheSameTaskIndex.remove(attempt);

    return otherAttemptsforTheSameTaskIndex.stream()
      .map(state -> (TaskState.State) state.getStateMachine().getCurrentState())
      .collect(Collectors.toList());
  }

  /**
   * (PRIVATE METHOD)
   * Updates the state of a stage.
   *
   * @param stageId       of the stage.
   * @param newStageState of the stage.
   */
  private void onStageStateChanged(final String stageId, final StageState.State newStageState) {
    // Change stage state
    final StateMachine stageStateMachine = stageIdToState.get(stageId).getStateMachine();

    metricStore.getOrCreateMetric(StageMetric.class, stageId)
      .addEvent(getStageState(stageId), newStageState);
    metricStore.triggerBroadcast(StageMetric.class, stageId);

    LOG.debug("Stage State Transition: id {} from {} to {}",
      new Object[]{stageId, stageStateMachine.getCurrentState(), newStageState});
    try {
      stageStateMachine.setState(newStageState);
    } catch (IllegalStateTransitionException e) {
      throw new RuntimeException(stageId + " - Illegal stage state transition ", e);
    }

    // Change plan state if needed
    final boolean allStagesCompleted = stageIdToState.values().stream().allMatch(state ->
      state.getStateMachine().getCurrentState().equals(StageState.State.COMPLETE));
    if (allStagesCompleted) {
      onPlanStateChanged(PlanState.State.COMPLETE);
    }
  }

  /**
   * (PRIVATE METHOD)
   * Updates the state of the plan.
   *
   * @param newState of the plan.
   */
  private void onPlanStateChanged(final PlanState.State newState) {
    metricStore.getOrCreateMetric(JobMetric.class, planId)
      .addEvent((PlanState.State) planState.getStateMachine().getCurrentState(), newState);
    metricStore.triggerBroadcast(JobMetric.class, planId);


    try {
      planState.getStateMachine().setState(newState);
    } catch (IllegalStateTransitionException e) {
      throw new RuntimeException(planId + " - Illegal plan state transition ", e);
    }

    if (newState == PlanState.State.EXECUTING) {
      LOG.debug("Executing Plan ID {}...", this.planId);
    } else if (newState == PlanState.State.COMPLETE || newState == PlanState.State.FAILED) {
      LOG.debug("Plan ID {} {}!", planId, newState);

      // Awake all threads waiting the finish of this plan.
      finishLock.lock();

      try {
        planFinishedCondition.signalAll();
      } finally {
        finishLock.unlock();
      }
    } else {
      throw new RuntimeException("Illegal Plan State Transition");
    }
  }


  /**
   * Wait for this plan to be finished and return the final state.
   *
   * @return the final state of this plan.
   */
  public PlanState.State waitUntilFinish() {
    finishLock.lock();
    try {
      while (!isPlanDone()) {
        planFinishedCondition.await();
      }
    } catch (final InterruptedException e) {
      LOG.warn("Interrupted during waiting the finish of Plan ID {}", planId);
      Thread.currentThread().interrupt();
    } finally {
      finishLock.unlock();
    }
    return getPlanState();
  }

  /**
   * Wait for this plan to be finished and return the final state.
   * It wait for at most the given time.
   *
   * @param timeout of waiting.
   * @param unit    of the timeout.
   * @return the final state of this plan.
   */
  public PlanState.State waitUntilFinish(final long timeout, final TimeUnit unit) {
    finishLock.lock();
    try {
      if (!isPlanDone()) {
        if (!planFinishedCondition.await(timeout, unit)) {
          LOG.warn("Timeout during waiting the finish of Plan ID {}", planId);
        }
      }
    } catch (final InterruptedException e) {
      LOG.warn("Interrupted during waiting the finish of Plan ID {}", planId);
      Thread.currentThread().interrupt();
    } finally {
      finishLock.unlock();
    }
    return getPlanState();
  }

  /**
   * @return whether the execution for the plan is done or not.
   */
  public synchronized boolean isPlanDone() {
    return (getPlanState() == PlanState.State.COMPLETE || getPlanState() == PlanState.State.FAILED);
  }

  /**
   * @return the ID of the plan.
   */
  public synchronized String getPlanId() {
    return planId;
  }

  /**
   * @return the state of the plan.
   */
  public synchronized PlanState.State getPlanState() {
    return (PlanState.State) planState.getStateMachine().getCurrentState();
  }

  /**
   * @param stageId the stage ID to query.
   * @return the state of the stage.
   */
  public synchronized StageState.State getStageState(final String stageId) {
    return (StageState.State) stageIdToState.get(stageId).getStateMachine().getCurrentState();
  }

  /**
   * @param taskId the ID of the task to query.
   * @return the state of the task.
   */
  public synchronized TaskState.State getTaskState(final String taskId) {
    return (TaskState.State) getTaskStateHelper(taskId).getStateMachine().getCurrentState();
  }

  private TaskState getTaskStateHelper(final String taskId) {
    return stageIdToTaskAttemptStates
      .get(RuntimeIdManager.getStageIdFromTaskId(taskId))
      .get(RuntimeIdManager.getIndexFromTaskId(taskId))
      .get(RuntimeIdManager.getAttemptFromTaskId(taskId));
  }

  /**
   * @return the physical plan.
   */
  public synchronized PhysicalPlan getPhysicalPlan() {
    return physicalPlan;
  }

  /**
   * @return the maximum number of task scheduling.
   */
  public int getMaxScheduleAttempt() {
    return maxScheduleAttempt;
  }

  /**
   * @return whether any plan has been submitted and initialized.
   */
  public synchronized boolean isInitialized() {
    return initialized;
  }

  /**
   * Stores JSON representation of plan state into a file.
   *
   * @param suffix suffix for file name
   */
  public void storeJSON(final String suffix) {
    if (dagDirectory.equals(EMPTY_DAG_DIRECTORY)) {
      return;
    }

    final File file = new File(dagDirectory, planId + "-" + dagLogFileIndex + "-" + suffix + ".json");
    file.getParentFile().mkdirs();
    try (final PrintWriter printWriter = new PrintWriter(file)) {
      printWriter.println(toStringWithPhysicalPlan());
      LOG.debug(String.format("JSON representation of plan state for %s(%s) was saved to %s",
        planId, dagLogFileIndex + "-" + suffix, file.getPath()));
    } catch (final IOException e) {
      LOG.warn(String.format("Cannot store JSON representation of plan state for %s(%s) to %s: %s",
        planId, dagLogFileIndex + "-" + suffix, file.getPath(), e.toString()));
    } finally {
      dagLogFileIndex++;
    }
  }

  private String toStringWithPhysicalPlan() {
    final StringBuilder sb = new StringBuilder("{");
    sb.append("\"dag\": ").append(physicalPlan.getStageDAG().toString()).append(", ");
    sb.append("\"planState\": ").append(toString()).append("}");
    return sb.toString();
  }

  @Override
  public synchronized String toString() {
    final StringBuilder sb = new StringBuilder("{");
    sb.append("\"planId\": \"").append(planId).append("\", ");
    sb.append("\"stages\": [");
    boolean isFirstStage = true;
    for (final Stage stage : physicalPlan.getStageDAG().getVertices()) {
      if (!isFirstStage) {
        sb.append(", ");
      }
      isFirstStage = false;
      final StageState stageState = stageIdToState.get(stage.getId());
      sb.append("{\"id\": \"").append(stage.getId()).append("\", ");
      sb.append("\"state\": \"").append(stageState.toString()).append("\", ");
      sb.append("\"tasks\": [");

      boolean isFirstTask = true;
      for (final Map.Entry<String, TaskState.State> entry : getTaskAttemptIdsToItsState(stage.getId()).entrySet()) {
        if (!isFirstTask) {
          sb.append(", ");
        }
        isFirstTask = false;
        sb.append("{\"id\": \"").append(entry.getKey()).append("\", ");
        sb.append("\"state\": \"").append(entry.getValue().toString()).append("\"}");
      }
      sb.append("]}");
    }
    sb.append("]}");
    return sb.toString();
  }

  @VisibleForTesting
  public synchronized Map<String, TaskState.State> getAllTaskAttemptIdsToItsState() {
    return physicalPlan.getStageDAG().getVertices()
      .stream()
      .map(Stage::getId)
      .flatMap(stageId -> getTaskAttemptIdsToItsState(stageId).entrySet().stream())
      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private Map<String, TaskState.State> getTaskAttemptIdsToItsState(final String stageId) {
    final Map<String, TaskState.State> result = new HashMap<>();
    final List<List<TaskState>> taskStates = stageIdToTaskAttemptStates.get(stageId);
    for (int taskIndex = 0; taskIndex < taskStates.size(); taskIndex++) {
      final List<TaskState> attemptStates = taskStates.get(taskIndex);
      for (int attempt = 0; attempt < attemptStates.size(); attempt++) {
        result.put(RuntimeIdManager.generateTaskId(stageId, taskIndex, attempt),
          (TaskState.State) attemptStates.get(attempt).getStateMachine().getCurrentState());
      }
    }
    return result;
  }
}
