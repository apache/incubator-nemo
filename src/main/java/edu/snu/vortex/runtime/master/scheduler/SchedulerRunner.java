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

import edu.snu.vortex.runtime.common.plan.physical.ScheduledTaskGroup;
import edu.snu.vortex.runtime.common.state.TaskGroupState;
import edu.snu.vortex.runtime.exception.SchedulingException;
import edu.snu.vortex.runtime.master.JobStateManager;
import org.apache.reef.annotations.audience.DriverSide;

import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Takes a TaskGroup from the pending queue and schedules it to an executor.
 */
@DriverSide
public final class SchedulerRunner implements Runnable {
  private static final Logger LOG = Logger.getLogger(SchedulerRunner.class.getName());
  private final JobStateManager jobStateManager;
  private final SchedulingPolicy schedulingPolicy;
  private final PendingTaskGroupQueue pendingTaskGroupQueue;

  SchedulerRunner(final JobStateManager jobStateManager,
                  final SchedulingPolicy schedulingPolicy,
                  final PendingTaskGroupQueue pendingTaskGroupQueue) {
    this.jobStateManager = jobStateManager;
    this.schedulingPolicy = schedulingPolicy;
    this.pendingTaskGroupQueue = pendingTaskGroupQueue;
  }

  /**
   * A separate thread is run to schedule task groups to executors.
   */
  @Override
  public void run() {
    // TODO #208: Check for Job Termination in a Cleaner Way
    while (!jobStateManager.checkJobCompletion()) {
      try {
        final ScheduledTaskGroup scheduledTaskGroup = pendingTaskGroupQueue.takeFirst();
        final Optional<String> executorId = schedulingPolicy.attemptSchedule(scheduledTaskGroup);
        if (!executorId.isPresent()) {
          LOG.log(Level.INFO, "Failed to assign an executor for {0} before the timeout: {1}",
              new Object[] {scheduledTaskGroup.getTaskGroup().getTaskGroupId(),
                  schedulingPolicy.getScheduleTimeoutMs()});
          pendingTaskGroupQueue.addLast(scheduledTaskGroup);
        } else {
          // Must send this scheduledTaskGroup to the destination executor.
          jobStateManager.onTaskGroupStateChanged(scheduledTaskGroup.getTaskGroup().getTaskGroupId(),
              TaskGroupState.State.EXECUTING);
          schedulingPolicy.onTaskGroupScheduled(executorId.get(), scheduledTaskGroup);
        }
      } catch (final Exception e) {
        throw new SchedulingException(e);
      }
    }
    LOG.log(Level.INFO, "Job is complete, scheduler runner will terminate.");
  }
}
