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

import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.runtime.common.plan.physical.*;
import edu.snu.vortex.runtime.common.state.StageState;
import edu.snu.vortex.runtime.common.state.TaskGroupState;
import edu.snu.vortex.runtime.exception.*;
import edu.snu.vortex.runtime.master.PartitionManagerMaster;
import edu.snu.vortex.runtime.master.JobStateManager;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * BatchScheduler receives a {@link PhysicalPlan} to execute and asynchronously schedules the task groups.
 * The policy by which it schedules them is dependent on the implementation of {@link SchedulingPolicy}.
 */
public final class BatchScheduler implements Scheduler {
  private static final Logger LOG = Logger.getLogger(BatchScheduler.class.getName());
  private static final int SCHEDULE_ATTEMPT_ON_CONTAINER_FAILURE = Integer.MAX_VALUE;

  private JobStateManager jobStateManager;

  /**
   * The {@link SchedulingPolicy} used to schedule task groups.
   */
  private SchedulingPolicy schedulingPolicy;

  private final PartitionManagerMaster partitionManagerMaster;

  private final PendingTaskGroupPriorityQueue pendingTaskGroupPriorityQueue;

  /**
   * The current job being executed.
   */
  private PhysicalPlan physicalPlan;

  @Inject
  public BatchScheduler(final PartitionManagerMaster partitionManagerMaster,
                        final SchedulingPolicy schedulingPolicy,
                        final PendingTaskGroupPriorityQueue pendingTaskGroupPriorityQueue) {
    this.partitionManagerMaster = partitionManagerMaster;
    this.pendingTaskGroupPriorityQueue = pendingTaskGroupPriorityQueue;
    this.schedulingPolicy = schedulingPolicy;
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

    LOG.log(Level.INFO, "Job to schedule: {0}", jobToSchedule.getId());

    // Launch scheduler
    final ExecutorService pendingTaskSchedulerThread = Executors.newSingleThreadExecutor();
    pendingTaskSchedulerThread.execute(
        new SchedulerRunner(jobStateManager, schedulingPolicy, pendingTaskGroupPriorityQueue));
    pendingTaskSchedulerThread.shutdown();

    scheduleRootStages();
    return jobStateManager;
  }

  /**
   * Receives a {@link edu.snu.vortex.runtime.common.comm.ControlMessage.TaskGroupStateChangedMsg} from an executor.
   * The message is received via communicator where this method is called.
   * @param executorId the id of the executor where the message was sent from.
   * @param taskGroupId whose state has changed
   * @param newState the state to change to
   * @param failedTaskIds if the task group failed. It is null otherwise.
   */
  @Override
  public void onTaskGroupStateChanged(final String executorId,
                                      final String taskGroupId,
                                      final TaskGroupState.State newState,
                                      final int attemptIdx,
                                      final List<String> failedTaskIds,
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
        onTaskGroupExecutionFailedRecoverable(executorId, taskGroup, failedTaskIds, failureCause);
      } else if (attemptIdx < attemptIndexForStage) {
        // if attemptIdx < attemptIndexForStage, we can ignore this late arriving message.
        LOG.log(Level.INFO, "{0} state change to failed_recoverable arrived late, we will ignore this.", taskGroupId);
      } else {
        throw new SchedulingException(new Throwable("AttemptIdx for a task group cannot be greater than its stage"));
      }
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

  private void onTaskGroupExecutionComplete(final String executorId,
                                            final TaskGroup taskGroup) {
    LOG.log(Level.INFO, "{0} completed in {1}", new Object[]{taskGroup.getTaskGroupId(), executorId});
    schedulingPolicy.onTaskGroupExecutionComplete(executorId, taskGroup.getTaskGroupId());
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

  private void onTaskGroupExecutionFailedRecoverable(final String executorId, final TaskGroup taskGroup,
                                                     final List<String> taskIdOnFailure,
                                                     final TaskGroupState.RecoverableFailureCause failureCause) {
    LOG.log(Level.INFO, "{0} failed in {1} by {2}", new Object[]{taskGroup.getTaskGroupId(), executorId, failureCause});
    schedulingPolicy.onTaskGroupExecutionFailed(executorId, taskGroup.getTaskGroupId());

    switch (failureCause) {
    // Previous task group must be re-executed, and incomplete task groups of the belonging stage must be rescheduled.
    case INPUT_READ_FAILURE:
      LOG.log(Level.INFO, "All task groups of {0} will be made failed_recoverable.", taskGroup.getStageId());
      for (final PhysicalStage stage : physicalPlan.getStageDAG().getTopologicalSort()) {
        if (stage.getId().equals(taskGroup.getStageId())) {
          LOG.log(Level.INFO, "Removing TaskGroups for {0} before they are scheduled to an executor", stage.getId());
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
      break;
    // The task group executed successfully but there is something wrong with the output store.
    case OUTPUT_WRITE_FAILURE:
    case CONTAINER_FAILURE:
      LOG.log(Level.INFO, "Only the failed task group will be retried.");
      break;
    default:
      throw new UnknownFailureCauseException(new Throwable("Unknown cause: " + failureCause));
    }

    // the stage this task group belongs to has become failed recoverable.
    // it is a good point to start searching for another stage to schedule.
    scheduleNextStage(taskGroup.getStageId());
  }

  @Override
  public synchronized void onExecutorAdded(final String executorId) {
    schedulingPolicy.onExecutorAdded(executorId);
  }

  @Override
  public synchronized void onExecutorRemoved(final String executorId) {
    final Set<String> taskGroupsForLostBlocks = partitionManagerMaster.removeWorker(executorId);
    final Set<String> taskGroupsToRecompute = schedulingPolicy.onExecutorRemoved(executorId);

    taskGroupsToRecompute.addAll(taskGroupsForLostBlocks);
    taskGroupsToRecompute.forEach(failedTaskGroupId -> {
      onTaskGroupStateChanged(executorId, failedTaskGroupId, TaskGroupState.State.FAILED_RECOVERABLE,
          SCHEDULE_ATTEMPT_ON_CONTAINER_FAILURE, null, TaskGroupState.RecoverableFailureCause.CONTAINER_FAILURE);
    });
  }

  private synchronized void scheduleRootStages() {
    final List<PhysicalStage> rootStages = physicalPlan.getStageDAG().getRootVertices();
    rootStages.forEach(this::scheduleStage);
  }

  /**
   * Schedules the next stage to execute after a stage completion.
   * @param completedStageId the ID of the stage that just completed and triggered this scheduling.
   */
  private synchronized void scheduleNextStage(final String completedStageId) {
    final List<PhysicalStage> childrenStages = physicalPlan.getStageDAG().getChildren(completedStageId);

    Optional<PhysicalStage> stageToSchedule;
    boolean scheduled = false;
    for (final PhysicalStage childStage : childrenStages) {
      stageToSchedule = selectNextStageToSchedule(childStage);
      if (stageToSchedule.isPresent()) {
        scheduled = true;
        scheduleStage(stageToSchedule.get());
        break;
      }
    }

    // No child stage has been selected, but there may be remaining stages.
    if (!scheduled) {
      for (final PhysicalStage stage : physicalPlan.getStageDAG().getTopologicalSort()) {
        final Enum stageState = jobStateManager.getStageState(stage.getId()).getStateMachine().getCurrentState();
        if (stageState == StageState.State.READY || stageState == StageState.State.FAILED_RECOVERABLE) {
          stageToSchedule = selectNextStageToSchedule(stage);
          if (stageToSchedule.isPresent()) {
            scheduleStage(stageToSchedule.get());
            break;
          }
        }
      }
    }
  }

  /**
   * Recursively selects the next stage to schedule.
   * For pull (all of the outputs from the parent stage must be ready before the next stage is scheduled):
   *    this method is triggered when a stage completes.
   * For push (at least one task group completes for the next stage to begin consuming the outputs):
   *    this method is triggered when a task group completes.
   * The selection mechanism is as follows:
   * a) The children stages become the candidates, each child stage given as the input to this method.
   * b) Examine the parent stages of the given stage, checking if all parent stages are complete.
   *      - If a parent stage has not yet been scheduled (state = READY),
   *        check its grandparent stages (with recursive calls to this method)
   *      - If a parent stage is executing (state = EXECUTING),
   *        then we must examine whether the outputs must be pushed,
   *        or there is nothing we can do but wait for it to complete.
   * c) When a stage to schedule is selected, return the stage.
   * A stage can only be scheduled when all parent stages connected by a “pull” edge are complete
   * and all those connected by a “push” edge are at least in the executing state.
   * @param stageTocheck the subject stage to check for scheduling.
   * @return the stage to schedule next.
   */
  private Optional<PhysicalStage> selectNextStageToSchedule(final PhysicalStage stageTocheck) {
    Optional<PhysicalStage> selectedStage = Optional.empty();
    final List<PhysicalStage> parentStageList = physicalPlan.getStageDAG().getParents(stageTocheck.getId());
    boolean safeToScheduleThisStage = true;
    for (final PhysicalStage parentStage : parentStageList) {
      final StageState.State parentStageState =
          (StageState.State) jobStateManager.getStageState(parentStage.getId()).getStateMachine().getCurrentState();

      switch (parentStageState) {
      case READY:
      case FAILED_RECOVERABLE:
        // look into see grandparent stages
        safeToScheduleThisStage = false;
        selectedStage = selectNextStageToSchedule(parentStage);
        break;
      case EXECUTING:
        final PhysicalStageEdge edgeFromParent =
            physicalPlan.getStageDAG().getEdgeBetween(parentStage.getId(), stageTocheck.getId());

        if (edgeFromParent.getAttributes().get(Attribute.Key.ChannelTransferPolicy) == Attribute.Pull) {
          // we cannot do anything but wait.
          safeToScheduleThisStage = false;
        } // else if the output of the parent stage is being pushed, we may be able to schedule this stage.
        break;
      case COMPLETE:
        break;
      case FAILED_UNRECOVERABLE:
        throw new UnrecoverableFailureException(new Throwable("Stage " + parentStage.getId()));
      default:
        throw new UnknownExecutionStateException(new Throwable("This stage state is unknown" + parentStageState));
      }

      // if a parent stage can be scheduled, select it.
      if (selectedStage.isPresent()) {
        return selectedStage;
      }
    }

    // if this stage can be scheduled after checking all parent stages,
    if (safeToScheduleThisStage) {
      selectedStage = Optional.of(stageTocheck);
    }
    return selectedStage;
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
    } else if (stageState == StageState.State.EXECUTING) {
      // An 'executing' stage has been selected as the next stage to execute.
      // This is due to the "Push" data transfer policy. We can skip this round.
      LOG.log(Level.INFO, "{0} has already been scheduled! Skipping this round.");
      return;
    }

    // attemptIdx is only initialized/updated when we set the stage's state to executing
    jobStateManager.onStageStateChanged(stageToSchedule.getId(), StageState.State.EXECUTING);
    final int attemptIdx = jobStateManager.getAttemptCountForStage(stageToSchedule.getId());
    LOG.log(Level.INFO, "Scheduling Stage {0} with attemptIdx={1}", new Object[]{stageToSchedule.getId(), attemptIdx});

    stageToSchedule.getTaskGroupList().forEach(taskGroup -> {
      // this happens when the belonging stage's other task groups have failed recoverable,
      // but this task group's results are safe.
      final TaskGroupState.State taskGroupState =
          (TaskGroupState.State)
              jobStateManager.getTaskGroupState(taskGroup.getTaskGroupId()).getStateMachine().getCurrentState();

      if (taskGroupState == TaskGroupState.State.COMPLETE || taskGroupState == TaskGroupState.State.EXECUTING) {
        LOG.log(Level.INFO, "Skipping {0} because its outputs are safe!", taskGroup.getTaskGroupId());
      } else {
        if (taskGroupState == TaskGroupState.State.FAILED_RECOVERABLE) {
          LOG.log(Level.INFO, "Re-scheduling {0} for failure recovery", taskGroup.getTaskGroupId());
          jobStateManager.onTaskGroupStateChanged(taskGroup, TaskGroupState.State.READY);
        }
        partitionManagerMaster.onProducerTaskGroupScheduled(taskGroup.getTaskGroupId());
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

  @Override
  public void terminate() {
  }
}
