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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

/**
 * Takes a TaskGroup from the pending queue and schedules it to an executor.
 */
@DriverSide
public final class SchedulerRunner {
  private static final Logger LOG = LoggerFactory.getLogger(SchedulerRunner.class.getName());
  private final Map<String, JobStateManager> jobStateManagers;
  private final SchedulingPolicy schedulingPolicy;
  private final PendingTaskGroupQueue pendingTaskGroupQueue;
  private final ExecutorService schedulerThread;
  private boolean initialJobScheduled;
  private boolean isTerminated;

  @Inject
  public SchedulerRunner(final SchedulingPolicy schedulingPolicy,
                         final PendingTaskGroupQueue pendingTaskGroupQueue) {
    this.jobStateManagers = new HashMap<>();
    this.pendingTaskGroupQueue = pendingTaskGroupQueue;
    this.schedulingPolicy = schedulingPolicy;
    this.schedulerThread = Executors.newSingleThreadExecutor();
    this.initialJobScheduled = false;
    this.isTerminated = false;
  }

  public synchronized void scheduleJob(final JobStateManager jobStateManager) {
    if (!isTerminated) {
      jobStateManagers.put(jobStateManager.getJobId(), jobStateManager);

      if (!initialJobScheduled) {
        initialJobScheduled = true;
        schedulerThread.execute(new SchedulerThread());
        schedulerThread.shutdown();
      }
    } // else ignore new incoming jobs when terminated.
  }

  public synchronized void terminate() {
    isTerminated = true;
  }

  /**
   * A separate thread is run to schedule task groups to executors.
   */
  private final class SchedulerThread implements Runnable {
    @Override
    public void run() {
      while (!isTerminated) {
        try {
          Optional<ScheduledTaskGroup> nextTaskGroupToSchedule;
          do {
            nextTaskGroupToSchedule = pendingTaskGroupQueue.dequeue();
          } while (!nextTaskGroupToSchedule.isPresent());

          final JobStateManager jobStateManager = jobStateManagers.get(nextTaskGroupToSchedule.get().getJobId());
          final boolean isScheduled =
              schedulingPolicy.scheduleTaskGroup(nextTaskGroupToSchedule.get(), jobStateManager);

          if (!isScheduled) {
            LOG.debug("Failed to assign an executor for {} before the timeout: {}",
                new Object[]{nextTaskGroupToSchedule.get().getTaskGroupId(),
                    schedulingPolicy.getScheduleTimeoutMs()});

            // Put this TaskGroup back to the queue since we failed to schedule it.
            pendingTaskGroupQueue.enqueue(nextTaskGroupToSchedule.get());
          }
        } catch (final Exception e) {
          e.printStackTrace();
          throw e;
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
}
