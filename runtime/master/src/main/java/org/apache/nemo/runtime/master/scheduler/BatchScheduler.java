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
package org.apache.nemo.runtime.master.scheduler;

import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.exception.UnknownExecutionStateException;
import org.apache.nemo.common.exception.UnrecoverableFailureException;
import org.apache.nemo.common.ir.vertex.executionproperty.ClonedSchedulingProperty;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.plan.*;
import org.apache.nemo.runtime.common.state.StageState;
import org.apache.nemo.runtime.common.state.TaskState;
import org.apache.nemo.runtime.master.BlockManagerMaster;
import org.apache.nemo.runtime.master.PlanAppender;
import org.apache.nemo.runtime.master.PlanStateManager;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;

/**
 * (CONCURRENCY) Only a single dedicated thread should use the public methods of this class.
 * (i.e., runtimeMasterThread in RuntimeMaster)
 * <p>
 * BatchScheduler receives a single {@link PhysicalPlan} to execute and schedules the Tasks.
 *
 * Note: When modifying this class, take a look at {@link SimulationScheduler}.
 */
@DriverSide
@NotThreadSafe
public final class BatchScheduler implements Scheduler {
  private static final Logger LOG = LoggerFactory.getLogger(BatchScheduler.class.getName());

  /**
   * Run-time optimizations.
   */
  private final PlanRewriter planRewriter;

  /**
   * Components related to scheduling the given plan.
   */
  private final TaskDispatcher taskDispatcher;  // Class for dispatching tasks.
  private final PendingTaskCollectionPointer pendingTaskCollectionPointer;  // A 'pointer' to the list of pending tasks.
  private final ExecutorRegistry executorRegistry;  // A registry for executors available for the job.
  private final PlanStateManager planStateManager;  // A component that manages the state of the plan.

  /**
   * Other necessary components of this {@link org.apache.nemo.runtime.master.RuntimeMaster}.
   */
  private final BlockManagerMaster blockManagerMaster;  // A component that manages data blocks.

  /**
   * The below variables depend on the submitted plan to execute.
   */
  private List<List<Stage>> sortedScheduleGroups;  // Stages, sorted in the order to be scheduled.

  @Inject
  private BatchScheduler(final PlanRewriter planRewriter,
                         final TaskDispatcher taskDispatcher,
                         final PendingTaskCollectionPointer pendingTaskCollectionPointer,
                         final BlockManagerMaster blockManagerMaster,
                         final ExecutorRegistry executorRegistry,
                         final PlanStateManager planStateManager) {
    this.planRewriter = planRewriter;
    this.taskDispatcher = taskDispatcher;
    this.pendingTaskCollectionPointer = pendingTaskCollectionPointer;
    this.blockManagerMaster = blockManagerMaster;
    this.executorRegistry = executorRegistry;
    this.planStateManager = planStateManager;
  }

  ////////////////////////////////////////////////////////////////////// Methods for plan rewriting.

  @Override
  public void updatePlan(final PhysicalPlan newPhysicalPlan) {
    // update the physical plan in the scheduler.
    // NOTE: what's already been executed is not modified in the new physical plan.
    // TODO #182: Consider reshaping in run-time optimization. At now, we only consider plan appending.
    updatePlan(newPhysicalPlan, planStateManager.getMaxScheduleAttempt());
  }

  /**
   * Update the physical plan in the scheduler.
   *
   * @param newPhysicalPlan    the new physical plan to update.
   * @param maxScheduleAttempt the maximum number of task scheduling attempt.
   */
  private void updatePlan(final PhysicalPlan newPhysicalPlan,
                          final int maxScheduleAttempt) {
    planStateManager.updatePlan(newPhysicalPlan, maxScheduleAttempt);
    this.sortedScheduleGroups = newPhysicalPlan.getStageDAG().getVertices().stream()
      .collect(Collectors.groupingBy(Stage::getScheduleGroup))
      .entrySet().stream()
      .sorted(Map.Entry.comparingByKey())
      .map(Map.Entry::getValue)
      .collect(Collectors.toList());
  }

  /**
   * Process the RuntimePassMessage.
   * @param taskId           that generated the message.
   * @param data             of the message.
   */
  public void onRunTimePassMessage(final String taskId, final Object data) {
    BatchSchedulerUtils.onRunTimePassMessage(planStateManager, planRewriter, taskId, data);
  }

  ////////////////////////////////////////////////////////////////////// Methods for scheduling.

  /**
   * Schedules a given plan.
   * If multiple physical plans are submitted, they will be appended and handled as a single plan.
   * TODO #182: Consider reshaping in run-time optimization. At now, we only consider plan appending.
   *
   * @param submittedPhysicalPlan the physical plan to schedule.
   * @param maxScheduleAttempt    the max number of times this plan/sub-part of the plan should be attempted.
   */
  @Override
  public void schedulePlan(final PhysicalPlan submittedPhysicalPlan,
                           final int maxScheduleAttempt) {
    LOG.info("Plan to schedule: {}", submittedPhysicalPlan.getPlanId());

    if (!planStateManager.isInitialized()) {
      // First scheduling.
      taskDispatcher.run();
      updatePlan(submittedPhysicalPlan, maxScheduleAttempt);
      planStateManager.storeJSON("submitted");
    } else {
      // Append the submitted plan to the original plan.
      final PhysicalPlan appendedPlan =
        PlanAppender.appendPlan(planStateManager.getPhysicalPlan(), submittedPhysicalPlan);
      updatePlan(appendedPlan, maxScheduleAttempt);
      planStateManager.storeJSON("appended");
    }

    doSchedule();
  }

  /**
   * Handles task state transition notifications sent from executors.
   * Note that we can receive notifications for previous task attempts, due to the nature of asynchronous events.
   * We ignore such late-arriving notifications, and only handle notifications for the current task attempt.
   *
   * @param executorId       the id of the executor where the message was sent from.
   * @param taskId           whose state has changed
   * @param taskAttemptIndex of the task whose state has changed
   * @param newState         the state to change to
   * @param vertexPutOnHold  the ID of vertex that is put on hold. It is null otherwise.
   */
  @Override
  public void onTaskStateReportFromExecutor(final String executorId,
                                            final String taskId,
                                            final int taskAttemptIndex,
                                            final TaskState.State newState,
                                            @Nullable final String vertexPutOnHold,
                                            final TaskState.RecoverableTaskFailureCause failureCause) {
    // Do change state, as this notification is for the current task attempt.
    planStateManager.onTaskStateChanged(taskId, newState);
    switch (newState) {
      case COMPLETE:
        BatchSchedulerUtils.onTaskExecutionComplete(executorRegistry, executorId, taskId);
        break;
      case SHOULD_RETRY:
        // SHOULD_RETRY from an executor means that the task ran into a recoverable failure
        BatchSchedulerUtils.onTaskExecutionFailedRecoverable(planStateManager, blockManagerMaster, executorRegistry,
          executorId, taskId, failureCause);
        break;
      case ON_HOLD:
        final Optional<PhysicalPlan> optionalPhysicalPlan =
          BatchSchedulerUtils
            .onTaskExecutionOnHold(planStateManager, executorRegistry, planRewriter, executorId, taskId);
        optionalPhysicalPlan.ifPresent(this::updatePlan);
        break;
      case FAILED:
        throw new UnrecoverableFailureException(new Exception(String.format("The plan failed on %s in %s",
          taskId, executorId)));
      case READY:
      case EXECUTING:
        throw new RuntimeException("The states READY/EXECUTING cannot occur at this point");
      default:
        throw new UnknownExecutionStateException(new Exception("This TaskState is unknown: " + newState));
    }

    // Invoke doSchedule()
    switch (newState) {
      case COMPLETE:
      case ON_HOLD:
        // If the stage has completed
        final String stageIdForTaskUponCompletion = RuntimeIdManager.getStageIdFromTaskId(taskId);
        if (planStateManager.getStageState(stageIdForTaskUponCompletion).equals(StageState.State.COMPLETE)
          && !planStateManager.isPlanDone()) {
          doSchedule();
        }
        break;
      case SHOULD_RETRY:
        // Do retry
        doSchedule();
        break;
      default:
        break;
    }

    // Invoke taskDispatcher.onExecutorSlotAvailable()
    switch (newState) {
      // These three states mean that a slot is made available.
      case COMPLETE:
      case ON_HOLD:
      case SHOULD_RETRY:
        taskDispatcher.onExecutorSlotAvailable();
        break;
      default:
        break;
    }
  }

  @Override
  public void onSpeculativeExecutionCheck() {
    MutableBoolean isNewCloneCreated = new MutableBoolean(false);

    BatchSchedulerUtils.selectEarliestSchedulableGroup(sortedScheduleGroups, planStateManager)
      .ifPresent(scheduleGroup ->
        scheduleGroup.stream().map(Stage::getId).forEach(stageId -> {
          final Stage stage = planStateManager.getPhysicalPlan().getStageDAG().getVertexById(stageId);

          // Only if the ClonedSchedulingProperty is set...
          stage.getPropertyValue(ClonedSchedulingProperty.class).ifPresent(cloneConf -> {
            if (!cloneConf.isUpFrontCloning()) { // Upfront cloning is already handled.
              isNewCloneCreated.setValue(doSpeculativeExecution(stage, cloneConf));
            }
          });
        }));

    if (isNewCloneCreated.booleanValue()) {
      doSchedule(); // Do schedule the new clone.
    }
  }

  @Override
  public void onExecutorAdded(final ExecutorRepresenter executorRepresenter) {
    LOG.info("{} added (node: {})", executorRepresenter.getExecutorId(), executorRepresenter.getNodeName());
    executorRegistry.registerExecutor(executorRepresenter);
    taskDispatcher.onExecutorSlotAvailable();
  }

  @Override
  public void onExecutorRemoved(final String executorId) {
    LOG.info("{} removed", executorId);
    blockManagerMaster.removeWorker(executorId);

    // These are tasks that were running at the time of executor removal.
    final Set<String> interruptedTasks = new HashSet<>();
    executorRegistry.updateExecutor(executorId, (executor, state) -> {
      interruptedTasks.addAll(executor.onExecutorFailed());
      return Pair.of(executor, ExecutorRegistry.ExecutorState.FAILED);
    });

    // Blocks of the interrupted tasks are failed.
    interruptedTasks.forEach(blockManagerMaster::onProducerTaskFailed);

    // Retry the interrupted tasks (and required parents)
    BatchSchedulerUtils.retryTasksAndRequiredParents(planStateManager, blockManagerMaster, interruptedTasks);

    // Trigger the scheduling of SHOULD_RETRY tasks in the earliest scheduleGroup
    doSchedule();
  }

  @Override
  public void terminate() {
    this.taskDispatcher.terminate();
    this.executorRegistry.terminate();
  }

  ////////////////////////////////////////////////////////////////////// Task launch methods.

  /**
   * The main entry point for task scheduling.
   * This operation can be invoked at any point during job execution, as it is designed to be free of side-effects.
   * <p>
   * These are the reasons why.
   * - We 'reset' {@link PendingTaskCollectionPointer}, and not 'add' new tasks to it
   * - We make {@link TaskDispatcher} dispatch only the tasks that are READY.
   */
  private void doSchedule() {
    final Optional<List<Stage>> earliest =
      BatchSchedulerUtils.selectEarliestSchedulableGroup(sortedScheduleGroups, planStateManager);

    if (earliest.isPresent()) {
      final List<Task> tasksToSchedule = earliest.get().stream()
        .flatMap(stage ->
          BatchSchedulerUtils.selectSchedulableTasks(planStateManager, blockManagerMaster, stage).stream())
        .collect(Collectors.toList());
      if (!tasksToSchedule.isEmpty()) {
        LOG.info("Scheduling some tasks in {}, which are in the same ScheduleGroup", tasksToSchedule.stream()
          .map(Task::getTaskId)
          .map(RuntimeIdManager::getStageIdFromTaskId)
          .collect(Collectors.toSet()));

        // Set the pointer to the schedulable tasks.
        pendingTaskCollectionPointer.setToOverwrite(tasksToSchedule);

        // Notify the dispatcher that a new collection is available.
        taskDispatcher.onNewPendingTaskCollectionAvailable();
      }
    } else {
      LOG.info("Skipping this round as no ScheduleGroup is schedulable.");
    }
  }


  ////////////////////////////////////////////////////////////////////// Task cloning methods.

  /**
   * @return true if a new clone is created.
   *         false otherwise.
   */
  private boolean doSpeculativeExecution(final Stage stage, final ClonedSchedulingProperty.CloneConf cloneConf) {
    final double fractionToWaitFor = cloneConf.getFractionToWaitFor();
    final Object[] completedTaskTimes = planStateManager.getCompletedTaskTimeListMs(stage.getId()).toArray();

    // Only after the fraction of the tasks are done...
    // Delayed cloning (aggressive)
    if (completedTaskTimes.length > 0
      && completedTaskTimes.length >= Math.round(stage.getTaskIndices().size() * fractionToWaitFor)) {
      // Only if the running task is considered a 'straggler'....
      Arrays.sort(completedTaskTimes);
      final long medianTime = (long) completedTaskTimes[completedTaskTimes.length / 2];
      final double medianTimeMultiplier = cloneConf.getMedianTimeMultiplier();
      final Map<String, Long> executingTaskToTime = planStateManager.getExecutingTaskToRunningTimeMs(stage.getId());

      return modifyStageNumCloneUsingMedianTime(
        stage.getId(), completedTaskTimes.length, medianTime, medianTimeMultiplier, executingTaskToTime);
    } else {
      return false;
    }
  }

  /**
   * @return true if the number of clones for the stage is modified.
   *         false otherwise.
   */
  private boolean modifyStageNumCloneUsingMedianTime(final String stageId,
                                                     final long numCompletedTasks,
                                                     final long medianTime,
                                                     final double medianTimeMultiplier,
                                                     final Map<String, Long> executingTaskToTime) {
    for (final Map.Entry<String, Long> entry : executingTaskToTime.entrySet()) {
      final long runningTime = entry.getValue();
      if (runningTime > Math.round(medianTime * medianTimeMultiplier)) {
        final String taskId = entry.getKey();
        final boolean isNumCloneModified = planStateManager
          .setNumOfClones(stageId, RuntimeIdManager.getIndexFromTaskId(taskId), 2);
        if (isNumCloneModified) {
          LOG.info("Cloned {}, because its running time {} (ms) is bigger than {} tasks' "
              + "(median) {} (ms) * (multiplier) {}", taskId, runningTime, numCompletedTasks,
            medianTime, medianTimeMultiplier);
          return true;
        }
      }
    }

    return false;
  }
}
