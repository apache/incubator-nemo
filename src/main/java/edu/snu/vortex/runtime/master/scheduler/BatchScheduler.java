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

import edu.snu.vortex.runtime.common.plan.physical.PhysicalPlan;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalStage;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalStageEdge;
import edu.snu.vortex.runtime.common.plan.physical.ScheduledTaskGroup;
import edu.snu.vortex.runtime.common.state.StageState;
import edu.snu.vortex.runtime.common.state.TaskGroupState;
import edu.snu.vortex.runtime.exception.IllegalStateTransitionException;
import edu.snu.vortex.runtime.exception.UnknownExecutionStateException;
import edu.snu.vortex.runtime.exception.UnrecoverableFailureException;
import edu.snu.vortex.runtime.master.BlockManagerMaster;
import edu.snu.vortex.runtime.master.ExecutorRepresenter;
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
// TODO #234: Add Unit Tests for Scheduler
public final class BatchScheduler implements Scheduler {
  private static final Logger LOG = Logger.getLogger(BatchScheduler.class.getName());

  private final PendingTaskGroupQueue pendingTaskGroupQueue;

  private JobStateManager jobStateManager;

  /**
   * A map of executor ID to the corresponding {@link ExecutorRepresenter}.
   * This object is synchronized as multiple threads can access and modify {@link ExecutorRepresenter}s.
   */
  // TODO #233: Introduce Container Manager
  private final Map<String, ExecutorRepresenter> executorRepresenterMap;

  /**
   * The {@link SchedulingPolicy} used to schedule task groups.
   */
  private SchedulingPolicy schedulingPolicy;

  /**
   * The current job being executed.
   */
  private PhysicalPlan physicalPlan;

  @Inject
  public BatchScheduler(final SchedulingPolicy schedulingPolicy,
                        final PendingTaskGroupQueue pendingTaskGroupQueue) {
    this.pendingTaskGroupQueue = pendingTaskGroupQueue;
    this.executorRepresenterMap = new HashMap<>();
    this.schedulingPolicy = schedulingPolicy;
  }

  /**
   * Receives a job to schedule.
   * @param jobToSchedule the physical plan for the job.
   * @return the {@link JobStateManager} to keep track of the submitted job's states.
   */
  @Override
  public synchronized JobStateManager scheduleJob(final PhysicalPlan jobToSchedule,
                                                  final BlockManagerMaster blockManagerMaster) {
    this.physicalPlan = jobToSchedule;
    this.jobStateManager = new JobStateManager(jobToSchedule, blockManagerMaster);

    // Launch scheduler
    final ExecutorService pendingTaskSchedulerThread = Executors.newSingleThreadExecutor();
    pendingTaskSchedulerThread.execute(new SchedulerRunner(jobStateManager, schedulingPolicy, pendingTaskGroupQueue));
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
                                      final List<String> failedTaskIds) {
    jobStateManager.onTaskGroupStateChanged(taskGroupId, newState);

    // TODO #233: Introduce Container Manager
    switch (newState) {
    case COMPLETE:
      synchronized (executorRepresenterMap) {
        onTaskGroupExecutionComplete(executorRepresenterMap.get(executorId), taskGroupId);
      }
      break;
    case FAILED_RECOVERABLE:
      synchronized (executorRepresenterMap) {
        onTaskGroupExecutionFailed(executorRepresenterMap.get(executorId), taskGroupId, failedTaskIds);
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

  private void onTaskGroupExecutionComplete(final ExecutorRepresenter executor,
                                            final String taskGroupId) {
    schedulingPolicy.onTaskGroupExecutionComplete(executor, taskGroupId);

    final Optional<String> stageIdForTaskGroupUponCompletion = jobStateManager.checkStageCompletion(taskGroupId);
    // if the stage this task group belongs to is complete,
    if (stageIdForTaskGroupUponCompletion.isPresent()) {
      if (!jobStateManager.checkJobCompletion()) { // and if the job is not yet complete,
        scheduleNextStage(stageIdForTaskGroupUponCompletion.get());
      }
    }
  }

  // TODO #163: Handle Fault Tolerance
  private void onTaskGroupExecutionFailed(final ExecutorRepresenter executor, final String taskGroupId,
                                          final List<String> taskIdOnFailure) {
    schedulingPolicy.onTaskGroupExecutionFailed(executor, taskGroupId);
  }

  @Override
  public void onExecutorAdded(final ExecutorRepresenter executor) {
    schedulingPolicy.onExecutorAdded(executor);
    synchronized (executorRepresenterMap) {
      executorRepresenterMap.put(executor.getExecutorId(), executor);
    }
  }

  // TODO #163: Handle Fault Tolerance
  @Override
  public void onExecutorRemoved(final ExecutorRepresenter executor) {
    // TODO #233: Introduce Container Manager
    synchronized (executorRepresenterMap) {
      executorRepresenterMap.remove(executor.getExecutorId());
    }
    final Set<String> taskGroupsToReschedule = schedulingPolicy.onExecutorRemoved(executor);

    // Reschedule taskGroupsToReschedule
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
        if (jobStateManager.getStageState(stage.getId()).getStateMachine().getCurrentState()
            == StageState.State.READY) {
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
   * The selection mechanism is as follows:
   * a) When a stage completes, its children stages become the candidates,
   *    each child stage given as the input to this method.
   * b) Examine the parent stages of the given stage, checking if all parent stages are complete.
   *      - If a parent stage has not yet been scheduled (state = READY),
   *        check its grandparent stages (with recursive calls to this method)
   *      - If a parent stage is executing (state = EXECUTING),
   *        there is nothing we can do but wait for it to complete.
   * c) When a stage to schedule is selected, return the stage.
   * @param stageTocheck the subject stage to check for scheduling.
   * @return the stage to schedule next.
   */
  // TODO #234: Add Unit Tests for Scheduler
  private Optional<PhysicalStage> selectNextStageToSchedule(final PhysicalStage stageTocheck) {
    Optional<PhysicalStage> selectedStage = Optional.empty();
    final List<PhysicalStage> parentStageList = physicalPlan.getStageDAG().getParents(stageTocheck.getId());
    boolean allParentStagesComplete = true;
    for (PhysicalStage parentStage : parentStageList) {
      final StageState.State parentStageState =
          (StageState.State) jobStateManager.getStageState(parentStage.getId()).getStateMachine().getCurrentState();

      switch (parentStageState) {
      case READY:
        // look into see grandparent stages
        allParentStagesComplete = false;
        selectedStage = selectNextStageToSchedule(parentStage);
        break;
      case EXECUTING:
        // we cannot do anything but wait.
        allParentStagesComplete = false;
        break;
      case COMPLETE:
        break;
      case FAILED_RECOVERABLE:
        // TODO #163: Handle Fault Tolerance
        allParentStagesComplete = false;
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

    // this stage can be scheduled if all parent stages have completed.
    if (allParentStagesComplete) {
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

    LOG.log(Level.INFO, "Scheduling Stage: {0}", stageToSchedule.getId());
    jobStateManager.onStageStateChanged(stageToSchedule.getId(), StageState.State.EXECUTING);

    stageToSchedule.getTaskGroupList().forEach(taskGroup ->
        pendingTaskGroupQueue.addLast(new ScheduledTaskGroup(taskGroup, stageIncomingEdges, stageOutgoingEdges)));
  }

  @Override
  public void terminate() {
   executorRepresenterMap.entrySet().stream().forEach(e -> e.getValue().shutDown());
  }
}
