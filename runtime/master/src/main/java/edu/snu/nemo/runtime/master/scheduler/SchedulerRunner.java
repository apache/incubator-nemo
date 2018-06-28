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

import com.google.common.annotations.VisibleForTesting;
import edu.snu.nemo.runtime.common.plan.Task;
import edu.snu.nemo.runtime.common.state.TaskState;
import edu.snu.nemo.runtime.master.JobStateManager;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
  private ExecutorRegistry executorRegistry;
  private SchedulingPolicy schedulingPolicy;

  @VisibleForTesting
  @Inject
  public SchedulerRunner(final SchedulingPolicy schedulingPolicy,
                         final PendingTaskCollectionPointer pendingTaskCollectionPointer,
                         final ExecutorRegistry executorRegistry) {
    this.jobStateManagers = new HashMap<>();
    this.pendingTaskCollectionPointer = pendingTaskCollectionPointer;
    this.schedulerThread = Executors.newSingleThreadExecutor(runnable -> new Thread(runnable, "SchedulerRunner"));
    this.isSchedulerRunning = false;
    this.isTerminated = false;
    this.executorRegistry = executorRegistry;
    this.schedulingPolicy = schedulingPolicy;
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

  void doScheduleTaskList() {
    final Optional<Collection<Task>> taskListOptional = pendingTaskCollectionPointer.getAndSetNull();
    if (!taskListOptional.isPresent()) {
      // Task list is empty
      LOG.debug("PendingTaskCollectionPointer is empty. Awaiting for more Tasks...");
      return;
    }

    final Collection<Task> taskList = taskListOptional.get();
    final List<Task> couldNotSchedule = new ArrayList<>();
    for (final Task task : taskList) {
      final JobStateManager jobStateManager = jobStateManagers.get(task.getJobId());
      if (!jobStateManager.getTaskState(task.getTaskId()).equals(TaskState.State.READY)) {
        // Guard against race conditions causing duplicate task launches
        LOG.debug("Skipping {} as it is not READY", task.getTaskId());
        continue;
      }

      LOG.debug("Trying to schedule {}...", task.getTaskId());
      executorRegistry.viewExecutors(executors -> {
        final Set<ExecutorRepresenter> candidateExecutors =
            schedulingPolicy.filterExecutorRepresenters(executors, task);
        final Optional<ExecutorRepresenter> firstCandidate = candidateExecutors.stream().findFirst();

        if (firstCandidate.isPresent()) {
          // update metadata first
          jobStateManager.onTaskStateChanged(task.getTaskId(), TaskState.State.EXECUTING);

          // send the task
          final ExecutorRepresenter selectedExecutor = firstCandidate.get();
          selectedExecutor.onTaskScheduled(task);
          LOG.info("{} scheduled to {}", new Object[]{task.getTaskId(), selectedExecutor.getExecutorId()});
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
   * Signals to the condition on executor availability.
   */
  void onAnExecutorAvailable() {
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
  void runSchedulerThread() {
    if (!isTerminated && !isSchedulerRunning) {
      schedulerThread.execute(new SchedulerThread());
      schedulerThread.shutdown();
      isSchedulerRunning = true;
    }
  }

  /**
   * Begin scheduling a job.
   * @param jobStateManager the corresponding {@link JobStateManager}
   */
  void scheduleJob(final JobStateManager jobStateManager) {
    if (!isTerminated) {
      jobStateManagers.put(jobStateManager.getJobId(), jobStateManager);
    } // else ignore new incoming jobs when terminated.
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
