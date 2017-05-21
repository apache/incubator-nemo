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
import edu.snu.vortex.runtime.common.state.StageState;
import edu.snu.vortex.runtime.common.state.TaskGroupState;
import edu.snu.vortex.runtime.exception.IllegalStateTransitionException;
import edu.snu.vortex.runtime.exception.SchedulingException;
import edu.snu.vortex.runtime.exception.UnknownExecutionStateException;
import edu.snu.vortex.runtime.exception.UnrecoverableFailureException;
import edu.snu.vortex.runtime.master.BlockManagerMaster;
import edu.snu.vortex.runtime.master.ExecutorRepresenter;
import edu.snu.vortex.runtime.master.JobStateManager;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

  private final PendingTaskGroupQueue pendingTaskGroupQueue;

  private JobStateManager jobStateManager;

  /**
   * A map of executor ID to the corresponding {@link ExecutorRepresenter}.
   * This object is synchronized as multiple threads can access and modify {@link ExecutorRepresenter}s.
   */
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

    scheduleNextStage();
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
  // TODO #83: Introduce Task Group Executor
  // TODO #94: Implement Distributed Communicator
  @Override
  public void onTaskGroupStateChanged(final String executorId,
                                      final String taskGroupId,
                                      final TaskGroupState.State newState,
                                      final List<String> failedTaskIds) {
    jobStateManager.onTaskGroupStateChanged(taskGroupId, newState);
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

    // if the current stage is complete,
    if (jobStateManager.checkCurrentStageCompletion()) {
      if (!jobStateManager.checkJobCompletion()) { // and if the job is not yet complete,
        scheduleNextStage();
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
  // TODO #85: Introduce Resource Manager
  @Override
  public void onExecutorRemoved(final ExecutorRepresenter executor) {
    synchronized (executorRepresenterMap) {
      executorRepresenterMap.remove(executor.getExecutorId());
    }
    final Set<String> taskGroupsToReschedule = schedulingPolicy.onExecutorRemoved(executor);

    // Reschedule taskGroupsToReschedule
  }

  /**
   * Schedules the next stage to execute.
   * It adds the list of task groups for the stage where the scheduler thread continuously polls from.
   */
  private void scheduleNextStage() {
    PhysicalStage nextStageToExecute = null;
    for (final PhysicalStage physicalStage : physicalPlan.getStageDAG().getTopologicalSort()) {
      if (jobStateManager.getStageState(physicalStage.getId()).getStateMachine().getCurrentState()
          == StageState.State.READY) {
        nextStageToExecute = physicalStage;
        break;
      }
    }
    if (nextStageToExecute != null) {
      LOG.log(Level.INFO, "Scheduling Stage: {0}", nextStageToExecute.getId());
      jobStateManager.onStageStateChanged(nextStageToExecute.getId(), StageState.State.EXECUTING);
      pendingTaskGroupQueue.addAll(nextStageToExecute.getTaskGroupList());
    } else {
      throw new SchedulingException(new Exception("There is no next stage to execute! "
          + "There must have been something wrong in setting execution states!"));
    }
  }

  @Override
  public void terminate() {
   executorRepresenterMap.entrySet().stream().forEach(e -> {
     e.getValue().shutDown();
   });
  }
}
