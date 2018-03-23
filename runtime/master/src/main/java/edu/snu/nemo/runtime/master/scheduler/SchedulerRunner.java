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
package edu.snu.nemo.runtime.master.scheduler;

import edu.snu.nemo.runtime.common.plan.physical.ScheduledTaskGroup;
import edu.snu.nemo.runtime.common.state.JobState;
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
 * Takes a TaskGroup from the pending queue and schedules it to an executor.
 */
@DriverSide
@NotThreadSafe
public final class SchedulerRunner {
  private static final Logger LOG = LoggerFactory.getLogger(SchedulerRunner.class.getName());
  private final Map<String, JobStateManager> jobStateManagers;
  private final SchedulingPolicy schedulingPolicy;
  private final PendingTaskGroupQueue pendingTaskGroupQueue;
  private final ExecutorService schedulerThread;
  private boolean initialJobScheduled;
  private boolean isTerminated;
  private final SignalQueueingCondition mustCheckSchedulingAvailabilityOrSchedulerTerminated
      = new SignalQueueingCondition();

  @Inject
  public SchedulerRunner(final SchedulingPolicy schedulingPolicy,
                         final PendingTaskGroupQueue pendingTaskGroupQueue) {
    this.jobStateManagers = new HashMap<>();
    this.pendingTaskGroupQueue = pendingTaskGroupQueue;
    this.schedulingPolicy = schedulingPolicy;
    this.schedulerThread = Executors.newSingleThreadExecutor(runnable -> new Thread(runnable, "SchedulerRunner"));
    this.initialJobScheduled = false;
    this.isTerminated = false;
  }

  /**
   * Signals to the condition on executor availability.
   */
  public void onAnExecutorAvailable() {
    mustCheckSchedulingAvailabilityOrSchedulerTerminated.signal();
  }

  /**
   * Signals to the condition on TaskGroup availability.
   */
  public void onATaskGroupAvailable() {
    mustCheckSchedulingAvailabilityOrSchedulerTerminated.signal();
  }

  /**
   * Begin scheduling a job.
   * @param jobStateManager the corresponding {@link JobStateManager}
   */
  void scheduleJob(final JobStateManager jobStateManager) {
    if (!isTerminated) {
      jobStateManagers.put(jobStateManager.getJobId(), jobStateManager);

      if (!initialJobScheduled) {
        initialJobScheduled = true;
        schedulerThread.execute(new SchedulerThread());
        schedulerThread.shutdown();
      }
    } // else ignore new incoming jobs when terminated.
  }

  void terminate() {
    schedulingPolicy.terminate();
    isTerminated = true;
    mustCheckSchedulingAvailabilityOrSchedulerTerminated.signal();
  }

  /**
   * A separate thread is run to schedule task groups to executors.
   */
  private final class SchedulerThread implements Runnable {
    @Override
    public void run() {
      // Run the first iteration unconditionally
      mustCheckSchedulingAvailabilityOrSchedulerTerminated.signal();

      while (!isTerminated) {
        // Iteration guard
        mustCheckSchedulingAvailabilityOrSchedulerTerminated.await();

        final Collection<ScheduledTaskGroup> schedulableTaskGroups = pendingTaskGroupQueue
            .peekSchedulableTaskGroups().orElse(null);
        if (schedulableTaskGroups == null) {
          // TaskGroup queue is empty
          LOG.debug("PendingTaskGroupQueue is empty. Awaiting for more TaskGroups...");
          continue;
        }

        int numScheduledTaskGroups = 0;
        for (final ScheduledTaskGroup schedulableTaskGroup : schedulableTaskGroups) {
          final JobStateManager jobStateManager = jobStateManagers.get(schedulableTaskGroup.getJobId());
          LOG.debug("Trying to schedule {}...", schedulableTaskGroup.getTaskGroupId());
          final boolean isScheduled =
              schedulingPolicy.scheduleTaskGroup(schedulableTaskGroup, jobStateManager);
          if (isScheduled) {
            LOG.debug("Successfully scheduled {}", schedulableTaskGroup.getTaskGroupId());
            pendingTaskGroupQueue.remove(schedulableTaskGroup.getTaskGroupId());
            numScheduledTaskGroups++;
          } else {
            LOG.debug("Failed to schedule {}", schedulableTaskGroup.getTaskGroupId());
          }
        }

        LOG.debug("Examined {} TaskGroups, scheduled {} TaskGroups",
            schedulableTaskGroups.size(), numScheduledTaskGroups);
        if (schedulableTaskGroups.size() == numScheduledTaskGroups) {
          // Scheduled all TaskGroups in the stage
          // Immediately run next iteration to check whether there is another schedulable stage
          LOG.debug("Trying to schedule next Stage in the ScheduleGroup (if any)...");
          mustCheckSchedulingAvailabilityOrSchedulerTerminated.signal();
        }
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
   * A {@link Condition} primitive that 'queues' signal.
   */
  private final class SignalQueueingCondition {
    private final AtomicBoolean hasQueuedSignal = new AtomicBoolean(false);
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();

    public void signal() {
      lock.lock();
      try {
        hasQueuedSignal.set(true);
        condition.signal();
      } finally {
        lock.unlock();
      }
    }

    public void await() {
      lock.lock();
      try {
        if (!hasQueuedSignal.get()) {
          condition.await();
        }
        hasQueuedSignal.set(false);
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      } finally {
        lock.unlock();
      }
    }
  }
}
