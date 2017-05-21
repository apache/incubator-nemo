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

import edu.snu.vortex.runtime.common.plan.physical.TaskGroup;
import edu.snu.vortex.runtime.common.state.TaskGroupState;
import edu.snu.vortex.runtime.exception.SchedulingException;
import edu.snu.vortex.runtime.master.ExecutorRepresenter;
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
        final TaskGroup taskGroup = pendingTaskGroupQueue.takeFirst();
        final Optional<ExecutorRepresenter> executor = schedulingPolicy.attemptSchedule(taskGroup);
        if (!executor.isPresent()) {
          LOG.log(Level.INFO, "Failed to assign an executor before the timeout: {0}",
              schedulingPolicy.getScheduleTimeoutMs());
          pendingTaskGroupQueue.addLast(taskGroup);
        } else {
          // Must send this taskGroup to the destination executor.
          jobStateManager.onTaskGroupStateChanged(taskGroup.getTaskGroupId(),
              TaskGroupState.State.EXECUTING);
          schedulingPolicy.onTaskGroupScheduled(executor.get(), taskGroup);
        }
      } catch (final Exception e) {
        throw new SchedulingException(e);
      }
    }
  }
}
