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
import edu.snu.nemo.runtime.common.plan.physical.ScheduledTask;
import edu.snu.nemo.runtime.common.state.JobState;
import edu.snu.nemo.runtime.common.state.TaskState;
import edu.snu.nemo.runtime.master.JobStateManager;
import org.apache.reef.annotations.audience.DriverSide;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;

/**
 * Takes a Task from the pending queue and schedules it to an executor.
 */
@DriverSide
@NotThreadSafe
public final class SchedulerRunner {
  private static final Logger LOG = LoggerFactory.getLogger(SchedulerRunner.class.getName());
  private final Map<String, JobStateManager> jobStateManagers;
  private final PendingTaskCollection pendingTaskCollection;
  private final ExecutorService schedulerThread;
  private AtomicBoolean isSchedulerRunning;
  private boolean isTerminated;
  private final DelayedSignalingCondition mustCheckSchedulingAvailabilityOrSchedulerTerminated
      = new DelayedSignalingCondition();
  private ExecutorRegistry executorRegistry;
  private SchedulingPolicy schedulingPolicy;

  @VisibleForTesting
  @Inject
  public SchedulerRunner(final SchedulingPolicy schedulingPolicy,
                         final PendingTaskCollection pendingTaskCollection,
                         final ExecutorRegistry executorRegistry) {
    this.jobStateManagers = new HashMap<>();
    this.pendingTaskCollection = pendingTaskCollection;
    this.schedulerThread = Executors.newSingleThreadExecutor(runnable -> new Thread(runnable, "SchedulerRunner"));
    this.isSchedulerRunning = new AtomicBoolean(false);
    this.isTerminated = false;
    this.executorRegistry = executorRegistry;
    this.schedulingPolicy = schedulingPolicy;
  }

  /**
   * Signals to the condition on executor availability.
   */
  public void onAnExecutorAvailable() {
    mustCheckSchedulingAvailabilityOrSchedulerTerminated.signal();
  }

  /**
   * Signals to the condition on Task availability.
   */
  public void onATaskAvailable() {
    mustCheckSchedulingAvailabilityOrSchedulerTerminated.signal();
  }

  /**
   * Run the scheduler thread.
   */
  void runSchedulerThread() {
    if (!isTerminated) {
      if (!isSchedulerRunning.getAndSet(true)) {
        schedulerThread.execute(new SchedulerThread());
        schedulerThread.shutdown();
      }
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
    mustCheckSchedulingAvailabilityOrSchedulerTerminated.signal();
  }

  void doScheduleTask() {
    final Collection<ScheduledTask> schedulableTasks = pendingTaskCollection.peekSchedulableTasks().orElse(null);
    if (schedulableTasks == null) {
      // Task queue is empty
      LOG.debug("PendingTaskCollection is empty. Awaiting for more Tasks...");
      return;
    }

    int numScheduledTasks = 0;
    for (final ScheduledTask schedulableTask : schedulableTasks) {
      final JobStateManager jobStateManager = jobStateManagers.get(schedulableTask.getJobId());
      LOG.debug("Trying to schedule {}...", schedulableTask.getTaskId());

      final boolean isScheduled = executorRegistry.registerTask(schedulingPolicy, schedulableTask);
      if (isScheduled) {
        pendingTaskCollection.remove(schedulableTask.getTaskId());
        jobStateManager.onTaskStateChanged(schedulableTask.getTaskId(), TaskState.State.EXECUTING);
        numScheduledTasks++;

        LOG.debug("Successfully scheduled {}", schedulableTask.getTaskId());
      } else {
        LOG.debug("Failed to schedule {}", schedulableTask.getTaskId());
      }
    }

    LOG.debug("Examined {} Tasks, scheduled {} Tasks", schedulableTasks.size(), numScheduledTasks);
    if (schedulableTasks.size() == numScheduledTasks) {
      // Scheduled all Tasks in the stage
      // Immediately run next iteration to check whether there is another schedulable stage
      LOG.debug("Trying to schedule next Stage in the ScheduleGroup (if any)...");
      mustCheckSchedulingAvailabilityOrSchedulerTerminated.signal();
    }
  }

  /**
   * A separate thread is run to schedule tasks to executors.
   */
  private final class SchedulerThread implements Runnable {
    @Override
    public void run() {
      // Run the first iteration unconditionally
      mustCheckSchedulingAvailabilityOrSchedulerTerminated.signal();

      while (!isTerminated) {
        // Iteration guard
        mustCheckSchedulingAvailabilityOrSchedulerTerminated.await();
        doScheduleTask();
      }
      jobStateManagers.values().forEach(jobStateManager -> {
        if (jobStateManager.getJobState().getStateMachine().getCurrentState() == JobState.State.COMPLETE) {
          LOG.info("{} is complete.", jobStateManager.getJobId());
        } else {
          LOG.info("{} is incomplete.", jobStateManager.getJobId());
        }
      });
      LOG.info("SchedulerRunner Terminated!");
    }
  }

  /**
   * A {@link Condition} that allows 'delayed' signaling.
   */
  private final class DelayedSignalingCondition {
    private final AtomicBoolean hasDelayedSignal = new AtomicBoolean(false);
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();

    /**
     * Signals to this condition. If no thread is awaiting for this condition,
     * signaling is delayed until the first next {@link #await} invocation.
     */
    public void signal() {
      lock.lock();
      try {
        hasDelayedSignal.set(true);
        condition.signal();
      } finally {
        lock.unlock();
      }
    }

    /**
     * Awaits to this condition. The thread will awake when there is a delayed signal,
     * or the next first {@link #signal} invocation.
     */
    public void await() {
      lock.lock();
      try {
        if (!hasDelayedSignal.get()) {
          condition.await();
        }
        hasDelayedSignal.set(false);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } finally {
        lock.unlock();
      }
    }
  }
}
