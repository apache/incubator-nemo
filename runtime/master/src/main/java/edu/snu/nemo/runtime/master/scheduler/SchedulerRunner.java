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

import edu.snu.nemo.common.HashRange;
import edu.snu.nemo.common.KeyRange;
import edu.snu.nemo.common.ir.edge.executionproperty.DataSkewMetricProperty;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.plan.StageEdge;
import edu.snu.nemo.runtime.common.plan.Task;
import edu.snu.nemo.runtime.common.state.TaskState;
import edu.snu.nemo.runtime.master.JobStateManager;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.reef.annotations.audience.DriverSide;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;

/**
 * Schedules tasks in discrete batches (scheduling iterations).
 * A scheduling iteration occurs under one of the following conditions
 * - An executor slot becomes available (for reasons such as task completion/failure, or executor addition)
 * - A new list of tasks become available (for reasons such as stage completion, task failure, or executor removal)
 */
@DriverSide
@NotThreadSafe
public final class SchedulerRunner {
  private static final Logger LOG = LoggerFactory.getLogger(SchedulerRunner.class.getName());
  private final Map<String, JobStateManager> jobStateManagers;
  private final PendingTaskCollectionPointer pendingTaskCollectionPointer;
  private final ExecutorService schedulerThread;
  private boolean isSchedulerRunning;
  private boolean isTerminated;

  private final DelayedSignalingCondition schedulingIteration = new DelayedSignalingCondition();
  private final ExecutorRegistry executorRegistry;
  private final SchedulingConstraintRegistry schedulingConstraintRegistry;
  private final SchedulingPolicy schedulingPolicy;

  @Inject
  private SchedulerRunner(final SchedulingConstraintRegistry schedulingConstraintRegistry,
                          final SchedulingPolicy schedulingPolicy,
                          final PendingTaskCollectionPointer pendingTaskCollectionPointer,
                          final ExecutorRegistry executorRegistry) {
    this.jobStateManagers = new HashMap<>();
    this.pendingTaskCollectionPointer = pendingTaskCollectionPointer;
    this.schedulerThread = Executors.newSingleThreadExecutor(runnable ->
        new Thread(runnable, "SchedulerRunner thread"));
    this.isSchedulerRunning = false;
    this.isTerminated = false;
    this.executorRegistry = executorRegistry;
    this.schedulingPolicy = schedulingPolicy;
    this.schedulingConstraintRegistry = schedulingConstraintRegistry;
  }

  /**
   * A separate thread is run to schedule tasks to executors.
   * See comments in the {@link Scheduler} for avoiding race conditions.
   */
  private final class SchedulerThread implements Runnable {
    @Override
    public void run() {
      while (!isTerminated) {
        doScheduleTaskList();
        schedulingIteration.await();
      }
      jobStateManagers.values().forEach(jobStateManager -> {
        if (jobStateManager.isJobDone()) {
          LOG.info("{} is complete.", jobStateManager.getJobId());
        } else {
          LOG.info("{} is incomplete.", jobStateManager.getJobId());
        }
      });
      LOG.info("SchedulerRunner Terminated!");
    }
  }

  public boolean hasSkewedData(final Task task) {
    final int taskIdx = RuntimeIdGenerator.getIndexFromTaskId(task.getTaskId());
    for (StageEdge inEdge : task.getTaskIncomingEdges()) {
      final Map<Integer, KeyRange> taskIdxToKeyRange =
          inEdge.getPropertyValue(DataSkewMetricProperty.class).get().getMetric();
      final KeyRange hashRange = taskIdxToKeyRange.get(taskIdx);
      if (((HashRange) hashRange).isSkewed()) {
        return true;
      }
    }
    return false;
  }

  void doScheduleTaskList() {
    final Optional<Collection<Task>> taskListOptional = pendingTaskCollectionPointer.getAndSetNull();
    if (!taskListOptional.isPresent()) {
      // Task list is empty
      LOG.debug("PendingTaskCollectionPointer is empty. Awaiting for more Tasks...");
      return;
    }

    final List<String> scheduledTasks = new ArrayList<>();
    final Collection<Task> taskList = taskListOptional.get();
    final List<Task> couldNotSchedule = new ArrayList<>();

    for (final Task task : taskList) {
      final JobStateManager jobStateManager = jobStateManagers.get(task.getJobId());
      if (!jobStateManager.getTaskState(task.getTaskId()).equals(TaskState.State.READY)) {
        // Guard against race conditions causing duplicate task launches
        LOG.debug("Skipping {} as it is not READY", task.getTaskId());
        continue;
      }
      final String taskId = task.getTaskId();

      executorRegistry.viewExecutors(executors -> {
        final MutableObject<Set<ExecutorRepresenter>> candidateExecutors = new MutableObject<>(executors);
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
          jobStateManager.onTaskStateChanged(task.getTaskId(), TaskState.State.EXECUTING);
          final boolean isSkewed = hasSkewedData(task);
          if (isSkewed) {
            LOG.info("Skewed {} scheduled to {} {}",
                task.getTaskId(), selectedExecutor.getExecutorId(), selectedExecutor.getNodeName());
          } else {
            LOG.info("{} scheduled to {} {}",
                task.getTaskId(), selectedExecutor.getExecutorId(), selectedExecutor.getNodeName());
          }
          // send the task
          selectedExecutor.onTaskScheduled(task);
          scheduledTasks.add(taskId);
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
    schedulingIteration.signal();
  }

  /**
   * Signals to the condition on the Task collection availability.
   */
  void onNewPendingTaskCollectionAvailable() {
    schedulingIteration.signal();
  }

  /**
   * Run the scheduler thread.
   */
  void run(final JobStateManager jobStateManager) {
    if (!isTerminated && !isSchedulerRunning) {
      jobStateManagers.put(jobStateManager.getJobId(), jobStateManager);
      schedulerThread.execute(new SchedulerThread());
      schedulerThread.shutdown();
      isSchedulerRunning = true;
    }
  }

  void terminate() {
    isTerminated = true;
    schedulingIteration.signal();
  }

  /**
   * A {@link Condition} that allows 'delayed' signaling.
   */
  private final class DelayedSignalingCondition {
    private boolean hasDelayedSignal = false;
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();

    /**
     * Signals to this condition. If no thread is awaiting for this condition,
     * signaling is delayed until the first next {@link #await} invocation.
     */
    void signal() {
      lock.lock();
      try {
        hasDelayedSignal = true;
        condition.signal();
      } finally {
        lock.unlock();
      }
    }

    /**
     * Awaits to this condition. The thread will awake when there is a delayed signal,
     * or the next first {@link #signal} invocation.
     */
    void await() {
      lock.lock();
      try {
        if (!hasDelayedSignal) {
          condition.await();
        }
        hasDelayedSignal = false;
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } finally {
        lock.unlock();
      }
    }
  }
}
