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
 * (CONCURRENCY) Only a single dedicated thread should use the public methods of this class.
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
  private final PendingTaskCollectionPointer pendingTaskCollectionPointer;
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
  private List<List<Stage>> sortedScheduleGroups;

  @Inject
  public BatchSingleJobScheduler(final SchedulerRunner schedulerRunner,
                                 final PendingTaskCollectionPointer pendingTaskCollectionPointer,
                                 final BlockManagerMaster blockManagerMaster,
                                 final PubSubEventHandlerWrapper pubSubEventHandlerWrapper,
                                 final UpdatePhysicalPlanEventHandler updatePhysicalPlanEventHandler,
                                 final ExecutorRegistry executorRegistry) {
    this.schedulerRunner = schedulerRunner;
    this.pendingTaskCollectionPointer = pendingTaskCollectionPointer;
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
   * @param physicalPlanOfJob of the job.
   * @param jobStateManagerOfJob of the job.
   */
  @Override
  public void scheduleJob(final PhysicalPlan physicalPlanOfJob, final JobStateManager jobStateManagerOfJob) {
    if (this.physicalPlan != null || this.jobStateManager != null) {
      throw new IllegalStateException("scheduleJob() has been called more than once");
    }

    this.physicalPlan = physicalPlanOfJob;
    this.jobStateManager = jobStateManagerOfJob;

    schedulerRunner.scheduleJob(jobStateManagerOfJob);
    schedulerRunner.runSchedulerThread();
    LOG.info("Job to schedule: {}", this.physicalPlan.getId());

    this.sortedScheduleGroups = this.physicalPlan.getStageDAG().getVertices()
        .stream()
        .collect(Collectors.groupingBy(Stage::getScheduleGroupIndex))
        .entrySet()
        .stream()
        .sorted(Map.Entry.comparingByKey())
        .map(Map.Entry::getValue)
        .collect(Collectors.toList());

    doSchedule();
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
        case SHOULD_RETRY:
          onTaskExecutionFailedRecoverable(executorId, taskId, failureCause);
          break;
        case ON_HOLD:
          onTaskExecutionOnHold(executorId, taskId, vertexPutOnHold);
          break;
        case FAILED:
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
      // Do not change state, as this report is from a previous task attempt.
      // For example, the master can receive a notification that an executor has been removed,
      // and then a notification that the task that was running in the removed executor has been completed.
      // In this case, if we do not consider the attempt number, the state changes from SHOULD_RETRY to COMPLETED,
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
      onTaskStateReportFromExecutor(executorId, failedTaskId, attemptIndex, TaskState.State.SHOULD_RETRY,
          null, TaskState.RecoverableFailureCause.CONTAINER_FAILURE);
    });

    if (!tasksToReExecute.isEmpty()) {
      // Schedule a stage after marking the necessary tasks to failed_recoverable.
      // The stage for one of the tasks that failed is a starting point to look
      // for the next stage to be scheduled.
      doSchedule();
    }
  }

  @Override
  public void terminate() {
    this.schedulerRunner.terminate();
    this.executorRegistry.terminate();
  }

  ////////////////////////////////////////////////////////////////////// Key methods for scheduling

  /**
   * The entry point for task scheduling.
   */
  private void doSchedule() {
    final Optional<List<Stage>> earliest = selectEarliestSchedulableGroup();

    if (earliest.isPresent()) {
      // Get schedulable tasks.
      final List<Task> tasksToSchedule = earliest.get().stream()
          .flatMap(stage -> selectSchedulableTasks(stage).stream())
          .collect(Collectors.toList());

      LOG.info("Attempting to schedule {} in the same ScheduleGroup",
          tasksToSchedule.stream().map(Task::getTaskId).collect(Collectors.toList()));

      // Set the pointer to the schedulable tasks.
      pendingTaskCollectionPointer.setToOverwrite(tasksToSchedule);

      // Notify the runner that a new collection is available.
      schedulerRunner.onNewPendingTaskCollectionAvailable();
    } else {
      LOG.info("Skipping this round as no ScheduleGroup is schedulable.");
    }
  }

  private Optional<List<Stage>> selectEarliestSchedulableGroup() {
    return sortedScheduleGroups.stream()
        .filter(scheduleGroup -> scheduleGroup.stream()
            .map(Stage::getId)
            .map(jobStateManager::getStageState)
            .anyMatch(state -> state.equals(StageState.State.SCHEDULABLE))) // any schedulable stage in the group
        .findFirst(); // selects the one with the smallest scheduling group index.
  }

  private List<Task> selectSchedulableTasks(final Stage stageToSchedule) {
    final List<StageEdge> stageIncomingEdges =
        physicalPlan.getStageDAG().getIncomingEdgesOf(stageToSchedule.getId());
    final List<StageEdge> stageOutgoingEdges =
        physicalPlan.getStageDAG().getOutgoingEdgesOf(stageToSchedule.getId());

    final List<String> taskIdsToSchedule = new LinkedList<>();
    for (final String taskId : stageToSchedule.getTaskIds()) {
      final TaskState.State taskState = jobStateManager.getTaskState(taskId);

      switch (taskState) {
        // Don't schedule these.
        case COMPLETE:
        case EXECUTING:
        case ON_HOLD:
          break;

        // These are schedulable.
        case SHOULD_RETRY:
          jobStateManager.onTaskStateChanged(taskId, READY);
        case READY:
          taskIdsToSchedule.add(taskId);
          break;

        // This shouldn't happen.
        default:
          throw new SchedulingException(new Throwable("Detected a FAILED Task"));
      }
    }

    // Create and return tasks.
    final List<Map<String, Readable>> vertexIdToReadables = stageToSchedule.getVertexIdToReadables();
    final List<Task> tasks = new ArrayList<>(taskIdsToSchedule.size());
    taskIdsToSchedule.forEach(taskId -> {
      blockManagerMaster.onProducerTaskScheduled(taskId); // Notify the block manager early for push edges.
      final int taskIdx = RuntimeIdGenerator.getIndexFromTaskId(taskId);
      final int attemptIdx = jobStateManager.getTaskAttempt(taskId);
      tasks.add(new Task(
          physicalPlan.getId(),
          taskId,
          attemptIdx,
          stageToSchedule.getExecutionProperties(),
          stageToSchedule.getSerializedIRDAG(),
          stageIncomingEdges,
          stageOutgoingEdges,
          vertexIdToReadables.get(taskIdx)));
    });
    return tasks;
  }

  ////////////////////////////////////////////////////////////////////// Task state change handlers

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
        doSchedule();
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
        doSchedule();
        break;
      case CONTAINER_FAILURE:
        LOG.info("Only the failed task will be retried.");
        break;
      default:
        throw new UnknownFailureCauseException(new Throwable("Unknown cause: " + failureCause));
    }
    schedulerRunner.onAnExecutorAvailable();
  }

  ////////////////////////////////////////////////////////////////////// Helper methods

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
}
