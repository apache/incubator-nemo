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
package edu.snu.vortex.runtime.master.scheduler;

import edu.snu.vortex.common.Pair;
import edu.snu.vortex.compiler.ir.MetricCollectionBarrierVertex;
import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.common.PubSubEventHandlerWrapper;
import edu.snu.vortex.runtime.master.eventhandler.DynamicOptimizationEvent;
import edu.snu.vortex.runtime.common.plan.physical.*;
import edu.snu.vortex.runtime.common.state.StageState;
import edu.snu.vortex.runtime.common.state.TaskGroupState;
import edu.snu.vortex.runtime.exception.*;
import edu.snu.vortex.runtime.master.PartitionManagerMaster;
import edu.snu.vortex.runtime.master.JobStateManager;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.slf4j.Logger;

import static edu.snu.vortex.runtime.common.state.TaskGroupState.State.ON_HOLD;

/**
 * BatchScheduler receives a {@link PhysicalPlan} to execute and asynchronously schedules the task groups.
 * The policy by which it schedules them is dependent on the implementation of {@link SchedulingPolicy}.
 */
public final class BatchScheduler implements Scheduler {
  private static final Logger LOG = LoggerFactory.getLogger(BatchScheduler.class.getName());
  private static final int SCHEDULE_ATTEMPT_ON_CONTAINER_FAILURE = Integer.MAX_VALUE;
  private int initialScheduleGroup;

  private JobStateManager jobStateManager;

  /**
   * The {@link SchedulingPolicy} used to schedule task groups.
   */
  private SchedulingPolicy schedulingPolicy;

  private final PartitionManagerMaster partitionManagerMaster;

  private final PendingTaskGroupPriorityQueue pendingTaskGroupPriorityQueue;

  private final PubSubEventHandlerWrapper pubSubEventHandlerWrapper;

  /**
   * The current job being executed.
   */
  private PhysicalPlan physicalPlan;

  @Inject
  public BatchScheduler(final PartitionManagerMaster partitionManagerMaster,
                        final SchedulingPolicy schedulingPolicy,
                        final PendingTaskGroupPriorityQueue pendingTaskGroupPriorityQueue,
                        final PubSubEventHandlerWrapper pubSubEventHandlerWrapper) {
    this.partitionManagerMaster = partitionManagerMaster;
    this.pendingTaskGroupPriorityQueue = pendingTaskGroupPriorityQueue;
    this.schedulingPolicy = schedulingPolicy;
    this.pubSubEventHandlerWrapper = pubSubEventHandlerWrapper;
  }

  /**
   * Receives a job to schedule.
   * @param jobToSchedule the physical plan for the job.
   * @return the {@link JobStateManager} to keep track of the submitted job's states.
   */
  @Override
  public synchronized JobStateManager scheduleJob(final PhysicalPlan jobToSchedule,
                                                  final int maxScheduleAttempt) {
    this.physicalPlan = jobToSchedule;
    this.jobStateManager = new JobStateManager(jobToSchedule, partitionManagerMaster, maxScheduleAttempt);
    pendingTaskGroupPriorityQueue.onJobScheduled(physicalPlan);

    LOG.info("Job to schedule: {}", jobToSchedule.getId());

    this.initialScheduleGroup = jobToSchedule.getStageDAG().getVertices().stream()
        .mapToInt(physicalStage -> physicalStage.getScheduleGroupIndex())
        .min().getAsInt();

    // Launch scheduler
    final ExecutorService pendingTaskSchedulerThread = Executors.newSingleThreadExecutor();
    pendingTaskSchedulerThread.execute(
        new SchedulerRunner(jobStateManager, schedulingPolicy, pendingTaskGroupPriorityQueue));
    pendingTaskSchedulerThread.shutdown();

    scheduleRootStages();
    return jobStateManager;
  }

  @Override
  public void updateJob(final PhysicalPlan newPhysicalPlan, final Pair<String, TaskGroup> taskInfo) {
    // update the job in the scheduler.
    // NOTE: what's already been executed is not modified in the new physical plan.
    this.physicalPlan = newPhysicalPlan;
    if (taskInfo != null) {
      onTaskGroupExecutionComplete(taskInfo.left(), taskInfo.right(), true);
    }
  }

  /**
   * Receives a {@link edu.snu.vortex.runtime.common.comm.ControlMessage.TaskGroupStateChangedMsg} from an executor.
   * The message is received via communicator where this method is called.
   * @param executorId the id of the executor where the message was sent from.
   * @param taskGroupId whose state has changed
   * @param newState the state to change to
   * @param tasksPutOnHold the IDs of tasks that are put on hold. It is null otherwise.
   */
  @Override
  public void onTaskGroupStateChanged(final String executorId,
                                      final String taskGroupId,
                                      final TaskGroupState.State newState,
                                      final int attemptIdx,
                                      final List<String> tasksPutOnHold,
                                      final TaskGroupState.RecoverableFailureCause failureCause) {
    final TaskGroup taskGroup = getTaskGroupById(taskGroupId);
    jobStateManager.onTaskGroupStateChanged(taskGroup, newState);

    switch (newState) {
    case COMPLETE:
      onTaskGroupExecutionComplete(executorId, taskGroup);
      break;
    case FAILED_RECOVERABLE:
      final int attemptIndexForStage =
          jobStateManager.getAttemptCountForStage(getTaskGroupById(taskGroupId).getStageId());
      if (attemptIdx == attemptIndexForStage || attemptIdx == SCHEDULE_ATTEMPT_ON_CONTAINER_FAILURE) {
        onTaskGroupExecutionFailedRecoverable(executorId, taskGroup, failureCause);
      } else if (attemptIdx < attemptIndexForStage) {
        // if attemptIdx < attemptIndexForStage, we can ignore this late arriving message.
        LOG.info("{} state change to failed_recoverable arrived late, we will ignore this.", taskGroupId);
      } else {
        throw new SchedulingException(new Throwable("AttemptIdx for a task group cannot be greater than its stage"));
      }
      break;
    case ON_HOLD:
      onTaskGroupExecutionOnHold(executorId, taskGroup, tasksPutOnHold);
      break;
    case FAILED_UNRECOVERABLE:
      throw new UnrecoverableFailureException(new Exception(new StringBuffer().append("The job failed on TaskGroup #")
          .append(taskGroupId).append(" in Executor ").append(executorId).toString()));
    case READY:
    case EXECUTING:
      throw new IllegalStateTransitionException(new Exception("The states READY/EXECUTING cannot occur at this point"));
    default:
      throw new UnknownExecutionStateException(new Exception("This TaskGroupState is unknown: " + newState));
    }
  }

  /**
   * Action after task group execution has been completed, not after it has been put on hold.
   * @param executorId id of the executor.
   * @param taskGroup task group completed.
   */
  private void onTaskGroupExecutionComplete(final String executorId,
                                            final TaskGroup taskGroup) {
    onTaskGroupExecutionComplete(executorId, taskGroup, false);
  }

  /**
   * Action after task group execution has been completed.
   * @param executorId id of the executor.
   * @param taskGroup task group completed.
   * @param isOnHoldToComplete whether or not if it is switched to complete after it has been on hold.
   */
  private void onTaskGroupExecutionComplete(final String executorId,
                                            final TaskGroup taskGroup,
                                            final Boolean isOnHoldToComplete) {
    LOG.debug("{} completed in {}", new Object[]{taskGroup.getTaskGroupId(), executorId});
    if (!isOnHoldToComplete) {
      schedulingPolicy.onTaskGroupExecutionComplete(executorId, taskGroup.getTaskGroupId());
    }
    final String stageIdForTaskGroupUponCompletion = taskGroup.getStageId();

    final boolean stageComplete =
        jobStateManager.checkStageCompletion(stageIdForTaskGroupUponCompletion);

    if (stageComplete) {
      // if the stage this task group belongs to is complete,
      if (!jobStateManager.checkJobTermination()) { // and if the job is not yet complete or failed,
        scheduleNextStage(stageIdForTaskGroupUponCompletion);
      }
    } else {
      // determine if at least one of the children stages must receive this task group's output as "push"
      final List<PhysicalStageEdge> outputsOfThisStage =
          physicalPlan.getStageDAG().getOutgoingEdgesOf(stageIdForTaskGroupUponCompletion);
      boolean pushOutput = false;
      for (PhysicalStageEdge outputEdge : outputsOfThisStage) {
        if (outputEdge.getAttributes().get(Attribute.Key.ChannelTransferPolicy) == Attribute.Push) {
          pushOutput = true;
          break;
        }
      }

      if (pushOutput) {
        scheduleNextStage(stageIdForTaskGroupUponCompletion);
      }
    }
  }

  /**
   * Action for after task group execution is put on hold.
   * @param executorId executor id.
   * @param taskGroup task group.
   * @param tasksPutOnHold the IDs of task that is put on hold.
   */
  private void onTaskGroupExecutionOnHold(final String executorId,
                                          final TaskGroup taskGroup,
                                          final List<String> tasksPutOnHold) {
    LOG.info("{} put on hold in {}", new Object[]{taskGroup.getTaskGroupId(), executorId});
    schedulingPolicy.onTaskGroupExecutionComplete(executorId, taskGroup.getTaskGroupId());
    final String stageIdForTaskGroupUponCompletion = taskGroup.getStageId();

    final boolean stageComplete =
        jobStateManager.checkStageCompletion(stageIdForTaskGroupUponCompletion);

    if (stageComplete) {
      // get optimization vertex from the task.
      final MetricCollectionBarrierVertex metricCollectionBarrierVertex =
          taskGroup.getTaskDAG().getVertices().stream() // get tasks list
              .filter(task -> tasksPutOnHold.contains(task.getId())) // find it
              .map(physicalPlan::getIRVertexOf) // get the corresponding IRVertex, the MetricCollectionBarrierVertex
              .filter(irVertex -> irVertex instanceof MetricCollectionBarrierVertex)
              .distinct()
              .map(irVertex -> (MetricCollectionBarrierVertex) irVertex) // convert types
              .findFirst().orElseThrow(() -> new RuntimeException(ON_HOLD.name() // get it
              + " called with failed task ids by some other task than "
              + MetricCollectionBarrierTask.class.getSimpleName()));
      // and we will use this vertex to perform metric collection and dynamic optimization.

      pubSubEventHandlerWrapper.getPubSubEventHandler().onNext(
          new DynamicOptimizationEvent(physicalPlan, metricCollectionBarrierVertex, Pair.of(executorId, taskGroup)));
    } else {
      onTaskGroupExecutionComplete(executorId, taskGroup, true);
    }
  }

  private void onTaskGroupExecutionFailedRecoverable(final String executorId, final TaskGroup taskGroup,
                                                     final TaskGroupState.RecoverableFailureCause failureCause) {
    LOG.info("{} failed in {} by {}", new Object[]{taskGroup.getTaskGroupId(), executorId, failureCause});
    schedulingPolicy.onTaskGroupExecutionFailed(executorId, taskGroup.getTaskGroupId());

    switch (failureCause) {
    // Previous task group must be re-executed, and incomplete task groups of the belonging stage must be rescheduled.
    case INPUT_READ_FAILURE:
      LOG.info("All task groups of {} will be made failed_recoverable.", taskGroup.getStageId());
      for (final PhysicalStage stage : physicalPlan.getStageDAG().getTopologicalSort()) {
        if (stage.getId().equals(taskGroup.getStageId())) {
          LOG.info("Removing TaskGroups for {} before they are scheduled to an executor", stage.getId());
          pendingTaskGroupPriorityQueue.removeStageAndDescendantsFromQueue(stage.getId());
          stage.getTaskGroupList().forEach(tg -> {
            if (jobStateManager.getTaskGroupState(tg.getTaskGroupId()).getStateMachine().getCurrentState()
                != TaskGroupState.State.COMPLETE) {
              jobStateManager.onTaskGroupStateChanged(tg, TaskGroupState.State.FAILED_RECOVERABLE);
              partitionManagerMaster.onProducerTaskGroupFailed(tg.getTaskGroupId());
            }
          });
          break;
        }
      }
      // the stage this task group belongs to has become failed recoverable.
      // it is a good point to start searching for another stage to schedule.
      scheduleNextStage(taskGroup.getStageId());
      break;
    // The task group executed successfully but there is something wrong with the output store.
    case OUTPUT_WRITE_FAILURE:
      // the stage this task group belongs to has become failed recoverable.
      // it is a good point to start searching for another stage to schedule.
      scheduleNextStage(taskGroup.getStageId());
      break;
    case CONTAINER_FAILURE:
      LOG.info("Only the failed task group will be retried.");
      break;
    default:
      throw new UnknownFailureCauseException(new Throwable("Unknown cause: " + failureCause));
    }
  }

  @Override
  public synchronized void onExecutorAdded(final String executorId) {
    schedulingPolicy.onExecutorAdded(executorId);
  }

  @Override
  public synchronized void onExecutorRemoved(final String executorId) {
    final Set<String> taskGroupsToReExecute = new HashSet<>();

    // TaskGroups for lost blocks
    taskGroupsToReExecute.addAll(partitionManagerMaster.removeWorker(executorId));

    // TaskGroups executing on the removed executor
    taskGroupsToReExecute.addAll(schedulingPolicy.onExecutorRemoved(executorId));

    taskGroupsToReExecute.forEach(failedTaskGroupId ->
      onTaskGroupStateChanged(executorId, failedTaskGroupId, TaskGroupState.State.FAILED_RECOVERABLE,
          SCHEDULE_ATTEMPT_ON_CONTAINER_FAILURE, null, TaskGroupState.RecoverableFailureCause.CONTAINER_FAILURE));

    if (!taskGroupsToReExecute.isEmpty()) {
      // Schedule a stage after marking the necessary task groups to failed_recoverable.
      // The stage for one of the task groups that failed is a starting point to look
      // for the next stage to be scheduled.
      scheduleNextStage(getTaskGroupById(taskGroupsToReExecute.iterator().next()).getStageId());
    }
  }

  private synchronized void scheduleRootStages() {
    final List<PhysicalStage> rootStages = physicalPlan.getStageDAG().filterVertices(
        physicalStage -> physicalStage.getScheduleGroupIndex() == initialScheduleGroup);
    rootStages.forEach(this::scheduleStage);
  }

  /**
   * Schedules the next stage to execute after a stage completion.
   * @param completedStageId the ID of the stage that just completed and triggered this scheduling.
   */
  private synchronized void scheduleNextStage(final String completedStageId) {
    final PhysicalStage completeOrFailedStage = getStageById(completedStageId);
    final Optional<List<PhysicalStage>> nextStagesToSchedule =
        selectNextStagesToSchedule(completeOrFailedStage.getScheduleGroupIndex());

    if (nextStagesToSchedule.isPresent()) {
      LOG.info("Scheduling: ScheduleGroup {}", nextStagesToSchedule.get().get(0).getScheduleGroupIndex());
      nextStagesToSchedule.get().forEach(this::scheduleStage);
    } else {
      LOG.info("Skipping this round as the next schedulable stages have already been scheduled.");
    }
  }

  /**
   * Selects the next list of stages to schedule.
   * This is a recursive function that decides which schedule group to schedule upon a stage completion, or a failure.
   * It takes the currentScheduleGroupIndex as a reference point to begin looking for the stages to execute:
   * a) returns the failed_recoverable stage(s) of the earliest schedule group, if it(they) exists.
   * b) returns an empty optional if there are no schedulable stages at the moment.
   *    - if the current schedule group is still executing
   *    - if an ancestor schedule group is still executing
   * c) returns the next set of schedulable stages (if the current schedule group has completed execution)
   * @param currentScheduleGroupIndex
   *      the index of the schedule group that is executing/has executed when this method is called.
   * @return an optional of the (possibly empty) list of next schedulable stages.
   */
  private Optional<List<PhysicalStage>> selectNextStagesToSchedule(final int currentScheduleGroupIndex) {
    if (currentScheduleGroupIndex > initialScheduleGroup) {
      final Optional<List<PhysicalStage>> ancestorStages = selectNextStagesToSchedule(currentScheduleGroupIndex - 1);
      if (ancestorStages.isPresent()) {
        return ancestorStages;
      }
    }

    // All previous schedule groups are complete, we need to check for the current schedule group.
    final List<PhysicalStage> currentScheduleGroup = physicalPlan.getStageDAG().filterVertices(
        physicalStage -> physicalStage.getScheduleGroupIndex() == currentScheduleGroupIndex);
    final List<PhysicalStage> stagesToSchedule = new LinkedList<>();
    boolean allStagesComplete = true;

    // We need to reschedule failed_recoverable stages.
    for (final PhysicalStage stageToCheck : currentScheduleGroup) {
      final StageState.State stageState =
          (StageState.State) jobStateManager.getStageState(stageToCheck.getId()).getStateMachine().getCurrentState();
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
    stagesToSchedule.addAll(physicalPlan.getStageDAG().filterVertices(
        physicalStage -> physicalStage.getScheduleGroupIndex() == currentScheduleGroupIndex + 1));

    final List<PhysicalStage> filteredStagesToSchedule =
        stagesToSchedule.stream().filter(physicalStage -> {
      final String stageId = physicalStage.getId();
      return jobStateManager.getStageState(stageId).getStateMachine().getCurrentState() != StageState.State.EXECUTING
          && jobStateManager.getStageState(stageId).getStateMachine().getCurrentState() != StageState.State.COMPLETE;
    }).collect(Collectors.toList());

    if (filteredStagesToSchedule.isEmpty()) {
      LOG.debug("ScheduleGroup {}: already executing/complete!, so we skip this", currentScheduleGroupIndex + 1);
      return Optional.empty();
    }
    return Optional.of(filteredStagesToSchedule);
  }

  /**
   * Schedules the given stage.
   * It adds the list of task groups for the stage where the scheduler thread continuously polls from.
   * @param stageToSchedule the stage to schedule.
   */
  private void scheduleStage(final PhysicalStage stageToSchedule) {
    final List<PhysicalStageEdge> stageIncomingEdges =
        physicalPlan.getStageDAG().getIncomingEdgesOf(stageToSchedule.getId());
    final List<PhysicalStageEdge> stageOutgoingEdges =
        physicalPlan.getStageDAG().getOutgoingEdgesOf(stageToSchedule.getId());

    final Enum stageState = jobStateManager.getStageState(stageToSchedule.getId()).getStateMachine().getCurrentState();

    if (stageState == StageState.State.FAILED_RECOVERABLE) {
      // The 'failed_recoverable' stage has been selected as the next stage to execute. Change its state back to 'ready'
      jobStateManager.onStageStateChanged(stageToSchedule.getId(), StageState.State.READY);
    }

    // attemptIdx is only initialized/updated when we set the stage's state to executing
    jobStateManager.onStageStateChanged(stageToSchedule.getId(), StageState.State.EXECUTING);
    final int attemptIdx = jobStateManager.getAttemptCountForStage(stageToSchedule.getId());
    LOG.info("Scheduling Stage {} with attemptIdx={}", new Object[]{stageToSchedule.getId(), attemptIdx});

    stageToSchedule.getTaskGroupList().forEach(taskGroup -> {
      // this happens when the belonging stage's other task groups have failed recoverable,
      // but this task group's results are safe.
      final TaskGroupState.State taskGroupState =
          (TaskGroupState.State)
              jobStateManager.getTaskGroupState(taskGroup.getTaskGroupId()).getStateMachine().getCurrentState();

      if (taskGroupState == TaskGroupState.State.COMPLETE || taskGroupState == TaskGroupState.State.EXECUTING) {
        LOG.info("Skipping {} because its outputs are safe!", taskGroup.getTaskGroupId());
      } else {
        if (taskGroupState == TaskGroupState.State.FAILED_RECOVERABLE) {
          LOG.info("Re-scheduling {} for failure recovery", taskGroup.getTaskGroupId());
          jobStateManager.onTaskGroupStateChanged(taskGroup, TaskGroupState.State.READY);
        }
        partitionManagerMaster.onProducerTaskGroupScheduled(taskGroup.getTaskGroupId());
        LOG.debug("Enquing {}", taskGroup.getTaskGroupId());
        pendingTaskGroupPriorityQueue.enqueue(
            new ScheduledTaskGroup(taskGroup, stageIncomingEdges, stageOutgoingEdges, attemptIdx));
      }
    });
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

  private PhysicalStage getStageById(final String stageId) {
    for (final PhysicalStage physicalStage : physicalPlan.getStageDAG().getVertices()) {
      if (physicalStage.getId().equals(stageId)) {
        return physicalStage;
      }
    }
    throw new RuntimeException(new Throwable("This taskGroupId does not exist in the plan"));
  }

  @Override
  public void terminate() {
  }
}
