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

import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.comm.ExecutorMessage;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalPlan;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalStage;
import edu.snu.vortex.runtime.common.plan.physical.TaskGroup;
import edu.snu.vortex.runtime.common.state.StageState;
import edu.snu.vortex.runtime.common.state.TaskGroupState;
import edu.snu.vortex.runtime.exception.IllegalStateTransitionException;
import edu.snu.vortex.runtime.exception.SchedulingException;
import edu.snu.vortex.runtime.exception.UnknownExecutionStateException;
import edu.snu.vortex.runtime.exception.UnrecoverableFailureException;
import edu.snu.vortex.runtime.master.ExecutionStateManager;
import edu.snu.vortex.runtime.master.ExecutorRepresenter;

import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * BatchScheduler receives a {@link PhysicalPlan} to execute and asynchronously schedules the task groups.
 * The policy by which it schedules them is dependent on the implementation of {@link SchedulingPolicy}.
 */
public final class BatchScheduler implements Scheduler {
  private static final Logger LOG = Logger.getLogger(BatchScheduler.class.getName());

  private final ExecutorService schedulerThread;
  private final BlockingDeque<TaskGroup> taskGroupsToSchedule;
  private ExecutionStateManager executionStateManager;

  /**
   * A map of executor ID to the corresponding {@link ExecutorRepresenter}.
   * This object is synchronized as multiple threads can access and modify {@link ExecutorRepresenter}s.
   */
  private final Map<String, ExecutorRepresenter> executorRepresenterMap;

  /**
   * The {@link SchedulingPolicy} used to schedule task groups.
   * {@link this#schedulingPolicyAttribute} decides the implementation.
   */
  private SchedulingPolicy schedulingPolicy;
  private RuntimeAttribute schedulingPolicyAttribute;

  /**
   * Timeout for the {@link SchedulingPolicy} upon task group scheduling.
   */
  private long scheduleTimeout;

  /**
   * The current job being executed.
   */
  private PhysicalPlan physicalPlan;

  public BatchScheduler(final RuntimeAttribute schedulingPolicyAttribute,
                        final long scheduleTimeout) {
    this.schedulerThread = Executors.newSingleThreadExecutor();
    this.taskGroupsToSchedule = new LinkedBlockingDeque<>();
    this.executorRepresenterMap = new HashMap<>();
    schedulerThread.execute(new TaskGroupScheduleHandler());

    // The default policy is initialized and set here.
    this.schedulingPolicyAttribute = schedulingPolicyAttribute;
    this.scheduleTimeout = scheduleTimeout;
    initializeSchedulingPolicy();
  }

  /**
   * Initializes the scheduling policy.
   * This can be called anytime during this scheduler's lifetime and the policy will change flexibly.
   */
  private void initializeSchedulingPolicy() {
    switch (schedulingPolicyAttribute) {
    case RoundRobin:
      this.schedulingPolicy = new RoundRobinSchedulingPolicy(scheduleTimeout);
      break;
    default:
      throw new SchedulingException(new Exception("The scheduling policy is unsupported by runtime"));
    }
  }

  /**
   * Receives a job to schedule.
   * @param jobToSchedule the physical plan for the job.
   * @return the {@link ExecutionStateManager} to keep track of the submitted job's states.
   */
  @Override
  public synchronized ExecutionStateManager scheduleJob(final PhysicalPlan jobToSchedule) {
    this.physicalPlan = jobToSchedule;
    this.executionStateManager = new ExecutionStateManager(jobToSchedule);
    scheduleNextStage();
    return executionStateManager;
  }

  /**
   * Receives a {@link edu.snu.vortex.runtime.common.comm.ExecutorMessage.TaskGroupStateChangedMsg} from an executor.
   * The message is received via communicator where this method is called.
   * @param executorId the id of the executor where the message was sent from.
   * @param message from the executor.
   */
  // TODO #83: Introduce Task Group Executor
  // TODO #94: Implement Distributed Communicator
  @Override
  public void onTaskGroupStateChanged(final String executorId,
                                      final ExecutorMessage.TaskGroupStateChangedMsg message) {
    final TaskGroupState.State newState = convertState(message.getState());
    executionStateManager.onTaskGroupStateChanged(message.getTaskGroupId(), newState);
    switch (newState) {
    case COMPLETE:
      synchronized (executorRepresenterMap) {
        onTaskGroupExecutionComplete(executorRepresenterMap.get(executorId),
            message.getTaskGroupId());
      }
      break;
    case FAILED_RECOVERABLE:
      synchronized (executorRepresenterMap) {
        onTaskGroupExecutionFailed(executorRepresenterMap.get(executorId), message.getTaskGroupId(),
            message.getFailedTaskIdsList());
      }
      break;
    case FAILED_UNRECOVERABLE:
      throw new UnrecoverableFailureException(new Exception(new StringBuffer().append("The job failed on TaskGroup #")
          .append(message.getTaskGroupId()).append(" in Executor ").append(executorId).toString()));
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
    if (executionStateManager.checkCurrentStageCompletion()) {
      if (!executionStateManager.checkJobCompletion()) { // and if the job is not yet complete,
        scheduleNextStage();
      }
    }
  }

  // TODO #163: Handle Fault Tolerance
  private void onTaskGroupExecutionFailed(final ExecutorRepresenter executor, final String taskGroupId,
                                          final List<String> taskIdOnFailure) {
    schedulingPolicy.onTaskGroupExecutionFailed(executor, taskGroupId);
  }

  // TODO #85: Introduce Resource Manager
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
      if (executionStateManager.getStageState(physicalStage.getId()).getStateMachine().getCurrentState()
          == StageState.State.READY) {
        nextStageToExecute = physicalStage;
        break;
      }
    }
    if (nextStageToExecute != null) {
      taskGroupsToSchedule.addAll(nextStageToExecute.getTaskGroupList());
      executionStateManager.onStageStateChanged(nextStageToExecute.getId(), StageState.State.EXECUTING);
    } else {
      throw new SchedulingException(new Exception("There is no next stage to execute! " +
          "There must have been something wrong in setting execution states!"));
    }
  }

  /**
   * A separate thread is run to schedule task groups to executors.
   */
  private class TaskGroupScheduleHandler implements Runnable {
    @Override
    public void run() {
      while (!schedulerThread.isShutdown()) {
        try {
          final TaskGroup taskGroup = taskGroupsToSchedule.takeFirst();
          final Optional<ExecutorRepresenter> executor = schedulingPolicy.attemptSchedule(taskGroup);
          if (!executor.isPresent()) {
            LOG.log(Level.INFO, "Failed to assign an executor before the timeout: {0}",
                schedulingPolicy.getScheduleTimeout());
            taskGroupsToSchedule.addLast(taskGroup);
          } else {
            // TODO #83: Introduce Task Group Executor
            // TODO #94: Implement Distributed Communicator
            // Must send this taskGroup to the destination executor.
            schedulingPolicy.onTaskGroupScheduled(executor.get(), taskGroup.getTaskGroupId());
            executionStateManager.onTaskGroupStateChanged(taskGroup.getTaskGroupId(),
                TaskGroupState.State.EXECUTING);
          }
        } catch (final Exception e) {
          throw new SchedulingException(e);
        }
      }
    }
  }

  @Override
  public void terminate() {
    schedulerThread.shutdown();
    taskGroupsToSchedule.clear();
  }

  // TODO #164: Cleanup Protobuf Usage
  private TaskGroupState.State convertState(final ExecutorMessage.TaskGroupStateFromExecutor state) {
    switch (state) {
    case READY:
      return TaskGroupState.State.READY;
    case EXECUTING:
      return TaskGroupState.State.EXECUTING;
    case COMPLETE:
      return TaskGroupState.State.COMPLETE;
    case FAILED_RECOVERABLE:
      return TaskGroupState.State.FAILED_RECOVERABLE;
    case FAILED_UNRECOVERABLE:
      return TaskGroupState.State.FAILED_UNRECOVERABLE;
    default:
      throw new UnknownExecutionStateException(new Exception("This TaskGroupState is unknown: " + state));
    }
  }
}
