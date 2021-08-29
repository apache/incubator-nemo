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

import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.nemo.common.exception.UnknownExecutionStateException;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.common.state.TaskState;
import org.apache.nemo.runtime.master.PlanStateManager;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Scheduler for simulating an execution not controlled by the runtime master.
 */
public abstract class ScheduleSimulator implements Scheduler {
  /**
   * Components related to scheduling the given plan. The role of each class can be found in {@link BatchScheduler}.
   */
  private TaskDispatchSimulator taskDispatcher;
  private final PendingTaskCollectionPointer pendingTaskCollectionPointer;
  private ExecutorRegistry executorRegistry;
  private PlanStateManager planStateManager;

  /**
   * Components that tell how to schedule the given tasks.
   */
  private final SchedulingConstraintRegistry schedulingConstraintRegistry;
  private final SchedulingPolicy schedulingPolicy;
  /**
   * String to generate simulated executors from.
   */
  private final String dagDirectory;


  @Inject
  public ScheduleSimulator(final SchedulingConstraintRegistry schedulingConstraintRegistry,
                           final SchedulingPolicy schedulingPolicy,
                           @Parameter(JobConf.DAGDirectory.class) final String dagDirectory) {
    this.pendingTaskCollectionPointer = PendingTaskCollectionPointer.newInstance();
    this.executorRegistry = ExecutorRegistry.newInstance();
    this.schedulingConstraintRegistry = schedulingConstraintRegistry;
    this.schedulingPolicy = schedulingPolicy;
    this.dagDirectory = dagDirectory;
    this.planStateManager = PlanStateManager.newInstance(dagDirectory);
    this.taskDispatcher = new TaskDispatchSimulator(schedulingConstraintRegistry, schedulingPolicy,
      pendingTaskCollectionPointer, executorRegistry, planStateManager);
  }

  /**
   * Reset the instance to its initial state.
   */
  public void reset() {
    this.terminate();
    this.executorRegistry = ExecutorRegistry.newInstance();
    this.planStateManager = PlanStateManager.newInstance(dagDirectory);
    this.pendingTaskCollectionPointer.getAndSetNull();
    this.taskDispatcher = new TaskDispatchSimulator(schedulingConstraintRegistry, schedulingPolicy,
      pendingTaskCollectionPointer, executorRegistry, planStateManager);
  }

  /**
   * get planStateManager.
   */
  public PlanStateManager getPlanStateManager() {
    return planStateManager;
  }

  /**
   * get pendingTaskCollectionPointer.
   */
  public PendingTaskCollectionPointer getPendingTaskCollectionPointer() {
    return pendingTaskCollectionPointer;
  }

  /**
   * get taskDispatcher.
   */
  public TaskDispatchSimulator getTaskDispatcher() {
    return taskDispatcher;
  }

  /**
   * Schedules a given plan.
   *
   * @param physicalPlan the physical plan to schedule.
   * @param maxScheduleAttempt    the max number of times this plan/sub-part of the plan should be attempted.
   */
  @Override
  public abstract void schedulePlan(PhysicalPlan physicalPlan, int maxScheduleAttempt);

  /**
   * Update the physical plan and maximum attempt.
   * But it is not supported yet.
   *
   * @param newPhysicalPlan    the physical plan to manage.
   */
  @Override
  public abstract void updatePlan(PhysicalPlan newPhysicalPlan);

  /**
   * Called when an executor is added to Simulator.
   *
   * @param executorRepresenter a representation of the added executor.
   */
  @Override
  public void onExecutorAdded(final ExecutorRepresenter executorRepresenter) {
    executorRegistry.registerExecutor(executorRepresenter);
    taskDispatcher.onExecutorSlotAvailable();
  }

  /**
   * Called when an executor is removed from Simulator.
   * But it is not supported yet.
   *
   * @param executorId of the executor that has been removed.
   */
  @Override
  public void onExecutorRemoved(final String executorId) {
    // we don't simulate executor removed case yet.
    throw new UnsupportedOperationException();
  }

  /**
   * Process the RuntimePassMessage.
   *
   * @param taskId that generated the message.
   * @param data   of the message.
   */
  public void onRunTimePassMessage(final String taskId, final Object data) {
    // we don't simulate runtime optimization yet.
    throw new UnsupportedOperationException();
  }

  /**
   * Handles task state transition notifications sent from executors.
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
    planStateManager.onTaskStateChanged(taskId, newState);

    switch (newState) {
      case COMPLETE:
        // Do nothing.
        break;
      case ON_HOLD:
      case FAILED:
      case SHOULD_RETRY:
        // TODO #226: StreamingScheduler Fault Tolerance
        throw new UnsupportedOperationException();
      case READY:
      case EXECUTING:
        throw new RuntimeException("The states READY/EXECUTING cannot occur at this point");
      default:
        throw new UnknownExecutionStateException(new Exception("This TaskState is unknown: " + newState));
    }
  }

  /**
   * Called to check for speculative execution.
   */
  @Override
  public void onSpeculativeExecutionCheck() {
    // we don't simulate speculate execution yet.
    return;
  }

  /**
   * To be called when a job should be terminated.
   * Any clean up code should be implemented in this method.
   */
  @Override
  public void terminate() {
    this.taskDispatcher.terminate();
    this.executorRegistry.terminate();
  }

  /**
   * TaskDispatcher for simulating an execution not controlled by the runtime master.
   * This class follows the structure of {@link TaskDispatcher}, so when a change has to be made on TaskDispatcher,
   * it also means that it should be reflected in this class as well.
   */
  static final class TaskDispatchSimulator {
    private static final Logger LOG = LoggerFactory.getLogger(TaskDispatcher.class.getName());
    private final PendingTaskCollectionPointer pendingTaskCollectionPointer;
    private PlanStateManager planStateManager;

    private final ExecutorRegistry executorRegistry;
    private final SchedulingConstraintRegistry schedulingConstraintRegistry;
    private final SchedulingPolicy schedulingPolicy;

    @Inject
    private TaskDispatchSimulator(final SchedulingConstraintRegistry schedulingConstraintRegistry,
                           final SchedulingPolicy schedulingPolicy,
                           final PendingTaskCollectionPointer pendingTaskCollectionPointer,
                           final ExecutorRegistry executorRegistry,
                           final PlanStateManager planStateManager) {
      this.pendingTaskCollectionPointer = pendingTaskCollectionPointer;
      this.planStateManager = planStateManager;
      this.executorRegistry = executorRegistry;
      this.schedulingPolicy = schedulingPolicy;
      this.schedulingConstraintRegistry = schedulingConstraintRegistry;
    }

    private void doScheduleTaskList() {
      final java.util.Optional<Collection<Task>> taskListOptional = pendingTaskCollectionPointer.getAndSetNull();
      if (!taskListOptional.isPresent()) {
        // Task list is empty
        LOG.debug("PendingTaskCollectionPointer is empty. Awaiting for more Tasks...");
        return;
      }

      final Collection<Task> taskList = taskListOptional.get();
      final List<Task> couldNotSchedule = new ArrayList<>();
      for (final Task task : taskList) {
        if (!planStateManager.getTaskState(task.getTaskId()).equals(TaskState.State.READY)) {
          // Guard against race conditions causing duplicate task launches
          LOG.debug("Skipping {} as it is not READY", task.getTaskId());
          continue;
        }

        executorRegistry.viewExecutors(executors -> {
          final MutableObject<Set<ExecutorRepresenter>> candidateExecutors = new MutableObject<>(executors);
          // Filter out the candidate executors that do not meet scheduling constraints.
          task.getExecutionProperties().forEachProperties(property -> {
            final Optional<SchedulingConstraint> constraint = schedulingConstraintRegistry.get(property.getClass());
            if (constraint.isPresent() && !candidateExecutors.getValue().isEmpty()) {
              candidateExecutors.setValue(candidateExecutors.getValue().stream()
                .filter(e -> constraint.get().testSchedulability(e, task))
                .collect(Collectors.toSet()));
            }
          });
          if (!candidateExecutors.getValue().isEmpty()) {
            // Select executor
            final ExecutorRepresenter selectedExecutor
              = schedulingPolicy.selectExecutor(candidateExecutors.getValue(), task);
            // update metadata first
            planStateManager.onTaskStateChanged(task.getTaskId(), TaskState.State.EXECUTING);

            LOG.info("{} scheduled to {}", task.getTaskId(), selectedExecutor.getExecutorId());
            // send the task
            selectedExecutor.onTaskScheduled(task);
          } else {
            couldNotSchedule.add(task);
          }
        });
      }

      LOG.debug("All except {} were scheduled among {}", new Object[]{couldNotSchedule, taskList});
      if (couldNotSchedule.size() > 0) {
        // Try these again, if no new task list has been set
        pendingTaskCollectionPointer.setIfNull(couldNotSchedule);
      }
    }

    /**
     * Signals to the condition on executor slot availability.
     */
    void onExecutorSlotAvailable() {
      doScheduleTaskList();
    }

    /**
     * Signals to the condition on the Task collection availability.
     */
    void onNewPendingTaskCollectionAvailable() {
      doScheduleTaskList();
    }

    void terminate() {
      doScheduleTaskList();
    }
  }
}
