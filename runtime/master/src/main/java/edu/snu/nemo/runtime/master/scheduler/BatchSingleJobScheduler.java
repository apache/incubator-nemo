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
package edu.snu.nemo.runtime.master.scheduler;

import edu.snu.nemo.common.Pair;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.eventhandler.PubSubEventHandlerWrapper;
import edu.snu.nemo.common.ir.Readable;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.eventhandler.DynamicOptimizationEvent;
import edu.snu.nemo.runtime.common.plan.*;
import edu.snu.nemo.runtime.common.state.TaskState;
import edu.snu.nemo.runtime.master.eventhandler.UpdatePhysicalPlanEventHandler;
import edu.snu.nemo.common.exception.*;
import edu.snu.nemo.common.ir.vertex.MetricCollectionBarrierVertex;
import edu.snu.nemo.runtime.common.state.StageState;
import edu.snu.nemo.runtime.master.BlockManagerMaster;
import edu.snu.nemo.runtime.master.JobStateManager;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;

import org.slf4j.Logger;

import static edu.snu.nemo.runtime.common.state.TaskState.State.ON_HOLD;
import static edu.snu.nemo.runtime.common.state.TaskState.State.READY;

/**
 * (WARNING) Only a single dedicated thread should use the public methods of this class.
 * (i.e., runtimeMasterThread in RuntimeMaster)
 *
 * BatchSingleJobScheduler receives a single {@link PhysicalPlan} to execute and schedules the Tasks.
 */
@DriverSide
@NotThreadSafe
public final class BatchSingleJobScheduler implements Scheduler {
  private static final Logger LOG = LoggerFactory.getLogger(BatchSingleJobScheduler.class.getName());

  /**
   * Components related to scheduling the given job.
   */
  private final SchedulerRunner schedulerRunner;
  private final PendingTaskCollection pendingTaskCollection;
  private final ExecutorRegistry executorRegistry;

  /**
   * Other necessary components of this {@link edu.snu.nemo.runtime.master.RuntimeMaster}.
   */
  private final BlockManagerMaster blockManagerMaster;
  private final PubSubEventHandlerWrapper pubSubEventHandlerWrapper;

  /**
   * The below variables depend on the submitted job to execute.
   */
  private PhysicalPlan physicalPlan;
  private JobStateManager jobStateManager;
  private int initialScheduleGroup;

  @Inject
  public BatchSingleJobScheduler(final SchedulerRunner schedulerRunner,
                                 final PendingTaskCollection pendingTaskCollection,
                                 final BlockManagerMaster blockManagerMaster,
                                 final PubSubEventHandlerWrapper pubSubEventHandlerWrapper,
                                 final UpdatePhysicalPlanEventHandler updatePhysicalPlanEventHandler,
                                 final ExecutorRegistry executorRegistry) {
    this.schedulerRunner = schedulerRunner;
    this.pendingTaskCollection = pendingTaskCollection;
    this.blockManagerMaster = blockManagerMaster;
    this.pubSubEventHandlerWrapper = pubSubEventHandlerWrapper;
    updatePhysicalPlanEventHandler.setScheduler(this);
    if (pubSubEventHandlerWrapper.getPubSubEventHandler() != null) {
      pubSubEventHandlerWrapper.getPubSubEventHandler()
          .subscribe(updatePhysicalPlanEventHandler.getEventClass(), updatePhysicalPlanEventHandler);
    }
    this.executorRegistry = executorRegistry;
  }

  /**
   * Receives a job to schedule.
   * @param jobToSchedule the physical plan for the job.
   * @param scheduledJobStateManager to keep track of the submitted job's states.
   */
  @Override
  public void scheduleJob(final PhysicalPlan jobToSchedule, final JobStateManager scheduledJobStateManager) {
    this.physicalPlan = jobToSchedule;
    this.jobStateManager = scheduledJobStateManager;

    schedulerRunner.scheduleJob(scheduledJobStateManager);
    schedulerRunner.runSchedulerThread();
    pendingTaskCollection.onJobScheduled(physicalPlan);

    LOG.info("Job to schedule: {}", jobToSchedule.getId());

    this.initialScheduleGroup = jobToSchedule.getStageDAG().getVertices().stream()
        .mapToInt(stage -> stage.getScheduleGroupIndex())
        .min().getAsInt();

    scheduleRootStages();
  }

  @Override
  public void updateJob(final String jobId, final PhysicalPlan newPhysicalPlan, final Pair<String, String> taskInfo) {
    // update the job in the scheduler.
    // NOTE: what's already been executed is not modified in the new physical plan.
    this.physicalPlan = newPhysicalPlan;
    if (taskInfo != null) {
      onTaskExecutionComplete(taskInfo.left(), taskInfo.right(), true);
    }
  }

  /**
   * Handles task state transition notifications sent from executors.
   * Note that we can receive notifications for previous task attempts, due to the nature of asynchronous events.
   * We ignore such late-arriving notifications, and only handle notifications for the current task attempt.
   *
   * @param executorId the id of the executor where the message was sent from.
   * @param taskId whose state has changed
   * @param taskAttemptIndex of the task whose state has changed
   * @param newState the state to change to
   * @param vertexPutOnHold the ID of vertex that is put on hold. It is null otherwise.
   */
  @Override
  public void onTaskStateReportFromExecutor(final String executorId,
                                            final String taskId,
                                            final int taskAttemptIndex,
                                            final TaskState.State newState,
                                            @Nullable final String vertexPutOnHold,
                                            final TaskState.RecoverableFailureCause failureCause) {
    final int currentTaskAttemptIndex = jobStateManager.getTaskAttempt(taskId);

    if (taskAttemptIndex == currentTaskAttemptIndex) {
      // Do change state, as this notification is for the current task attempt.
      jobStateManager.onTaskStateChanged(taskId, newState);
      switch (newState) {
        case COMPLETE:
          onTaskExecutionComplete(executorId, taskId);
          break;
        case FAILED_RECOVERABLE:
          onTaskExecutionFailedRecoverable(executorId, taskId, failureCause);
          break;
        case ON_HOLD:
          onTaskExecutionOnHold(executorId, taskId, vertexPutOnHold);
          break;
        case FAILED_UNRECOVERABLE:
          throw new UnrecoverableFailureException(new Exception(new StringBuffer().append("The job failed on Task #")
              .append(taskId).append(" in Executor ").append(executorId).toString()));
        case READY:
        case EXECUTING:
          throw new IllegalStateTransitionException(
              new Exception("The states READY/EXECUTING cannot occur at this point"));
        default:
          throw new UnknownExecutionStateException(new Exception("This TaskState is unknown: " + newState));
      }
    } else if (taskAttemptIndex < currentTaskAttemptIndex) {
      // Do not change state, as this notification is for a previous task attempt.
      // For example, the master can receive a notification that an executor has been removed,
      // and then a notification that the task that was running in the removed executor has been completed.
      // In this case, if we do not consider the attempt number, the state changes from FAILED_RECOVERABLE to COMPLETED,
      // which is illegal.
      LOG.info("{} state change to {} arrived late, we will ignore this.", new Object[]{taskId, newState});
    } else {
      throw new SchedulingException(new Throwable("AttemptIdx for a task cannot be greater than its current index"));
    }
  }

  @Override
  public void onExecutorAdded(final ExecutorRepresenter executorRepresenter) {
    executorRegistry.registerExecutor(executorRepresenter);
    schedulerRunner.onAnExecutorAvailable();
  }

  @Override
  public void onExecutorRemoved(final String executorId) {
    final Set<String> tasksToReExecute = new HashSet<>();
    // Tasks for lost blocks
    tasksToReExecute.addAll(blockManagerMaster.removeWorker(executorId));

    // Tasks executing on the removed executor
    executorRegistry.updateExecutor(executorId, (executor, state) -> {
      tasksToReExecute.addAll(executor.onExecutorFailed());
      return Pair.of(executor, ExecutorRegistry.ExecutorState.FAILED);
    });

    tasksToReExecute.forEach(failedTaskId -> {
      final int attemptIndex = jobStateManager.getTaskAttempt(failedTaskId);
      onTaskStateReportFromExecutor(executorId, failedTaskId, attemptIndex, TaskState.State.FAILED_RECOVERABLE,
          null, TaskState.RecoverableFailureCause.CONTAINER_FAILURE);
    });

    if (!tasksToReExecute.isEmpty()) {
      // Schedule a stage after marking the necessary tasks to failed_recoverable.
      // The stage for one of the tasks that failed is a starting point to look
      // for the next stage to be scheduled.
      scheduleNextStage(RuntimeIdGenerator.getStageIdFromTaskId(tasksToReExecute.iterator().next()));
    }
  }

  @Override
  public void terminate() {
    this.schedulerRunner.terminate();
    this.executorRegistry.terminate();
    this.pendingTaskCollection.close();
  }

  /**
   * Schedule stages in initial schedule group, in reverse-topological order.
   */
  private void scheduleRootStages() {
    final List<Stage> rootStages =
        physicalPlan.getStageDAG().getTopologicalSort().stream().filter(stage ->
            stage.getScheduleGroupIndex() == initialScheduleGroup)
            .collect(Collectors.toList());
    Collections.reverse(rootStages);
    rootStages.forEach(this::scheduleStage);
  }

  /**
   * Schedules the next stage to execute after a stage completion.
   * @param completedStageId the ID of the stage that just completed and triggered this scheduling.
   */
  private void scheduleNextStage(final String completedStageId) {
    final Stage completeOrFailedStage = getStageById(completedStageId);
    final Optional<List<Stage>> nextStagesToSchedule =
        selectNextStagesToSchedule(completeOrFailedStage.getScheduleGroupIndex());

    if (nextStagesToSchedule.isPresent()) {
      LOG.info("Scheduling: ScheduleGroup {}", nextStagesToSchedule.get().get(0).getScheduleGroupIndex());

      nextStagesToSchedule.get().forEach(this::scheduleStage);
    } else {
      LOG.info("Skipping this round as the next schedulable stages have already been scheduled.");
    }
  }

  /**
   * Selects the list of stages to schedule, in the order they must be added to {@link PendingTaskCollection}.
   *
   * This is a recursive function that decides which schedule group to schedule upon a stage completion, or a failure.
   * It takes the currentScheduleGroupIndex as a reference point to begin looking for the stages to execute:
   * a) returns the failed_recoverable stage(s) of the earliest schedule group, if it(they) exists.
   * b) returns an empty optional if there are no schedulable stages at the moment.
   *    - if the current schedule group is still executing
   *    - if an ancestor schedule group is still executing
   * c) returns the next set of schedulable stages (if the current schedule group has completed execution)
   *
   * The current implementation assumes that the stages that belong to the same schedule group are
   * either mutually independent, or connected by a "push" edge.
   *
   * @param currentScheduleGroupIndex
   *      the index of the schedule group that is executing/has executed when this method is called.
   * @return an optional of the (possibly empty) list of next schedulable stages, in the order they should be
   * enqueued to {@link PendingTaskCollection}.
   */
  private Optional<List<Stage>> selectNextStagesToSchedule(final int currentScheduleGroupIndex) {
    if (currentScheduleGroupIndex > initialScheduleGroup) {
      final Optional<List<Stage>> ancestorStagesFromAScheduleGroup =
          selectNextStagesToSchedule(currentScheduleGroupIndex - 1);
      if (ancestorStagesFromAScheduleGroup.isPresent()) {
        return ancestorStagesFromAScheduleGroup;
      }
    }

    // All previous schedule groups are complete, we need to check for the current schedule group.
    final List<Stage> currentScheduleGroup =
        physicalPlan.getStageDAG().getTopologicalSort().stream().filter(stage ->
            stage.getScheduleGroupIndex() == currentScheduleGroupIndex)
            .collect(Collectors.toList());
    List<Stage> stagesToSchedule = new LinkedList<>();
    boolean allStagesComplete = true;

    // We need to reschedule failed_recoverable stages.
    for (final Stage stageToCheck : currentScheduleGroup) {
      final StageState.State stageState = jobStateManager.getStageState(stageToCheck.getId());
      switch (stageState) {
        case FAILED_RECOVERABLE:
          stagesToSchedule.add(stageToCheck);
          allStagesComplete = false;
          break;
        case READY:
        case EXECUTING:
          allStagesComplete = false;
          break;
        default:
          break;
      }
    }
    if (!allStagesComplete) {
      LOG.info("There are remaining stages in the current schedule group, {}", currentScheduleGroupIndex);
      return (stagesToSchedule.isEmpty()) ? Optional.empty() : Optional.of(stagesToSchedule);
    }

    // By the time the control flow has reached here,
    // we are ready to move onto the next ScheduleGroup
    stagesToSchedule =
        physicalPlan.getStageDAG().getTopologicalSort().stream().filter(stage -> {
          if (stage.getScheduleGroupIndex() == currentScheduleGroupIndex + 1) {
            final String stageId = stage.getId();
            return jobStateManager.getStageState(stageId) != StageState.State.EXECUTING
                && jobStateManager.getStageState(stageId) != StageState.State.COMPLETE;
          }
          return false;
        }).collect(Collectors.toList());

    if (stagesToSchedule.isEmpty()) {
      LOG.debug("ScheduleGroup {}: already executing/complete!, so we skip this", currentScheduleGroupIndex + 1);
      return Optional.empty();
    }

    // Return the schedulable stage list in reverse-topological order
    // since the stages that belong to the same schedule group are mutually independent,
    // or connected by a "push" edge, requiring the children stages to be scheduled first.
    Collections.reverse(stagesToSchedule);
    return Optional.of(stagesToSchedule);
  }

  /**
   * Schedules the given stage.
   * It adds the list of tasks for the stage where the scheduler thread continuously polls from.
   * @param stageToSchedule the stage to schedule.
   */
  private void scheduleStage(final Stage stageToSchedule) {
    final List<StageEdge> stageIncomingEdges =
        physicalPlan.getStageDAG().getIncomingEdgesOf(stageToSchedule.getId());
    final List<StageEdge> stageOutgoingEdges =
        physicalPlan.getStageDAG().getOutgoingEdgesOf(stageToSchedule.getId());

    final StageState.State stageState = jobStateManager.getStageState(stageToSchedule.getId());

    final List<String> taskIdsToSchedule = new LinkedList<>();
    for (final String taskId : stageToSchedule.getTaskIds()) {
      // this happens when the belonging stage's other tasks have failed recoverable,
      // but this task's results are safe.
      final TaskState.State taskState = jobStateManager.getTaskState(taskId);

      switch (taskState) {
        case COMPLETE:
        case EXECUTING:
          LOG.info("Skipping {} because its outputs are safe!", taskId);
          break;
        case READY:
          if (stageState == StageState.State.FAILED_RECOVERABLE) {
            LOG.info("Skipping {} because it is already in the queue, but just hasn't been scheduled yet!",
                taskId);
          } else {
            LOG.info("Scheduling {}", taskId);
            taskIdsToSchedule.add(taskId);
          }
          break;
        case FAILED_RECOVERABLE:
          LOG.info("Re-scheduling {} for failure recovery", taskId);
          jobStateManager.onTaskStateChanged(taskId, READY);
          taskIdsToSchedule.add(taskId);
          break;
        case ON_HOLD:
          // Do nothing
          break;
        default:
          throw new SchedulingException(new Throwable("Detected a FAILED_UNRECOVERABLE Task"));
      }
    }

    LOG.info("Scheduling Stage {}", stageToSchedule.getId());

    // each readable and source task will be bounded in executor.
    final List<Map<String, Readable>> vertexIdToReadables = stageToSchedule.getVertexIdToReadables();

    taskIdsToSchedule.forEach(taskId -> {
      blockManagerMaster.onProducerTaskScheduled(taskId);
      final int taskIdx = RuntimeIdGenerator.getIndexFromTaskId(taskId);
      final int attemptIdx = jobStateManager.getTaskAttempt(taskId);

      LOG.debug("Enqueueing {}", taskId);
      pendingTaskCollection.add(new Task(
          physicalPlan.getId(),
          taskId,
          attemptIdx,
          stageToSchedule.getExecutionProperties(),
          stageToSchedule.getSerializedIRDAG(),
          stageIncomingEdges,
          stageOutgoingEdges,
          vertexIdToReadables.get(taskIdx)));
    });
    schedulerRunner.onATaskAvailable();
  }

  /**
   * @param taskId id of the task
   * @return the IR dag
   */
  private DAG<IRVertex, RuntimeEdge<IRVertex>> getVertexDagById(final String taskId) {
    for (final Stage stage : physicalPlan.getStageDAG().getVertices()) {
      if (stage.getId().equals(RuntimeIdGenerator.getStageIdFromTaskId(taskId))) {
        return stage.getIRDAG();
      }
    }
    throw new RuntimeException(new Throwable("This taskId does not exist in the plan"));
  }

  private Stage getStageById(final String stageId) {
    for (final Stage stage : physicalPlan.getStageDAG().getVertices()) {
      if (stage.getId().equals(stageId)) {
        return stage;
      }
    }
    throw new RuntimeException(new Throwable("This taskId does not exist in the plan"));
  }

  /**
   * Action after task execution has been completed, not after it has been put on hold.
   *
   * @param executorId  the ID of the executor.
   * @param taskId the ID pf the task completed.
   */
  private void onTaskExecutionComplete(final String executorId,
                                       final String taskId) {
    onTaskExecutionComplete(executorId, taskId, false);
  }

  /**
   * Action after task execution has been completed.
   * @param executorId id of the executor.
   * @param taskId the ID of the task completed.
   * @param isOnHoldToComplete whether or not if it is switched to complete after it has been on hold.
   */
  private void onTaskExecutionComplete(final String executorId,
                                       final String taskId,
                                       final Boolean isOnHoldToComplete) {
    LOG.debug("{} completed in {}", new Object[]{taskId, executorId});
    if (!isOnHoldToComplete) {
      executorRegistry.updateExecutor(executorId, (executor, state) -> {
        executor.onTaskExecutionComplete(taskId);
        return Pair.of(executor, state);
      });
    }

    final String stageIdForTaskUponCompletion = RuntimeIdGenerator.getStageIdFromTaskId(taskId);
    if (jobStateManager.getStageState(stageIdForTaskUponCompletion).equals(StageState.State.COMPLETE)) {
      // if the stage this task belongs to is complete,
      if (!jobStateManager.isJobDone()) {
        scheduleNextStage(stageIdForTaskUponCompletion);
      }
    }
    schedulerRunner.onAnExecutorAvailable();
  }

  /**
   * Action for after task execution is put on hold.
   * @param executorId       the ID of the executor.
   * @param taskId           the ID of the task.
   * @param vertexPutOnHold  the ID of vertex that is put on hold.
   */
  private void onTaskExecutionOnHold(final String executorId,
                                     final String taskId,
                                     final String vertexPutOnHold) {
    LOG.info("{} put on hold in {}", new Object[]{taskId, executorId});
    executorRegistry.updateExecutor(executorId, (executor, state) -> {
      executor.onTaskExecutionComplete(taskId);
      return Pair.of(executor, state);
    });
    final String stageIdForTaskUponCompletion = RuntimeIdGenerator.getStageIdFromTaskId(taskId);

    final boolean stageComplete =
        jobStateManager.getStageState(stageIdForTaskUponCompletion).equals(StageState.State.COMPLETE);

    if (stageComplete) {
      // get optimization vertex from the task.
      final MetricCollectionBarrierVertex metricCollectionBarrierVertex =
          getVertexDagById(taskId).getVertices().stream() // get vertex list
              .filter(irVertex -> irVertex.getId().equals(vertexPutOnHold)) // find it
              .filter(irVertex -> irVertex instanceof MetricCollectionBarrierVertex)
              .distinct()
              .map(irVertex -> (MetricCollectionBarrierVertex) irVertex) // convert types
              .findFirst().orElseThrow(() -> new RuntimeException(ON_HOLD.name() // get it
              + " called with failed task ids by some other task than "
              + MetricCollectionBarrierVertex.class.getSimpleName()));
      // and we will use this vertex to perform metric collection and dynamic optimization.

      pubSubEventHandlerWrapper.getPubSubEventHandler().onNext(
          new DynamicOptimizationEvent(physicalPlan, metricCollectionBarrierVertex, Pair.of(executorId, taskId)));
    } else {
      onTaskExecutionComplete(executorId, taskId, true);
    }
    schedulerRunner.onAnExecutorAvailable();
  }

  /**
   * Action for after task execution has failed but it's recoverable.
   * @param executorId    the ID of the executor
   * @param taskId   the ID of the task
   * @param failureCause  the cause of failure
   */
  private void onTaskExecutionFailedRecoverable(final String executorId,
                                                final String taskId,
                                                final TaskState.RecoverableFailureCause failureCause) {
    LOG.info("{} failed in {} by {}", taskId, executorId, failureCause);
    executorRegistry.updateExecutor(executorId, (executor, state) -> {
      executor.onTaskExecutionFailed(taskId);
      return Pair.of(executor, state);
    });

    final String stageId = RuntimeIdGenerator.getStageIdFromTaskId(taskId);

    switch (failureCause) {
      // Previous task must be re-executed, and incomplete tasks of the belonging stage must be rescheduled.
      case INPUT_READ_FAILURE:
        // TODO #50: Carefully retry tasks in the scheduler
      case OUTPUT_WRITE_FAILURE:
        blockManagerMaster.onProducerTaskFailed(taskId);
        scheduleNextStage(stageId);
        break;
      case CONTAINER_FAILURE:
        LOG.info("Only the failed task will be retried.");
        break;
      default:
        throw new UnknownFailureCauseException(new Throwable("Unknown cause: " + failureCause));
    }
    schedulerRunner.onAnExecutorAvailable();
  }
}
