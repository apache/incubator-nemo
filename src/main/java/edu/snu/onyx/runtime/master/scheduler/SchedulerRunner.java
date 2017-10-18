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
package edu.snu.onyx.runtime.master.scheduler;

import edu.snu.onyx.runtime.common.plan.physical.ScheduledTaskGroup;
import edu.snu.onyx.runtime.common.state.JobState;
import edu.snu.onyx.runtime.common.state.TaskGroupState;
import edu.snu.onyx.runtime.master.JobStateManager;
import edu.snu.onyx.runtime.master.resource.ContainerManager;
import org.apache.reef.annotations.audience.DriverSide;

import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Takes a TaskGroup from the pending queue and schedules it to an executor.
 */
@DriverSide
public final class SchedulerRunner implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(SchedulerRunner.class.getName());
  private final JobStateManager jobStateManager;
  private final SchedulingPolicy schedulingPolicy;
  private final PendingTaskGroupPriorityQueue pendingTaskGroupPriorityQueue;
  private final ContainerManager containerManager;

  SchedulerRunner(final JobStateManager jobStateManager,
                  final SchedulingPolicy schedulingPolicy,
                  final PendingTaskGroupPriorityQueue pendingTaskGroupPriorityQueue,
                  final ContainerManager containerManager) {
    this.jobStateManager = jobStateManager;
    this.schedulingPolicy = schedulingPolicy;
    this.pendingTaskGroupPriorityQueue = pendingTaskGroupPriorityQueue;
    this.containerManager = containerManager;
  }

  /**
   * A separate thread is run to schedule task groups to executors.
   */
  @Override
  public void run() {
    while (!jobStateManager.checkJobTermination()) {
      try {
        Optional<ScheduledTaskGroup> nextTaskGroupToSchedule;
        do {
          nextTaskGroupToSchedule = pendingTaskGroupPriorityQueue.dequeueNextTaskGroup();
        } while (!nextTaskGroupToSchedule.isPresent());

        final Optional<String> executorId = schedulingPolicy.attemptSchedule(nextTaskGroupToSchedule.get());
        if (!executorId.isPresent()) {
          LOG.info("Failed to assign an executor for {} before the timeout: {}",
              new Object[] {nextTaskGroupToSchedule.get().getTaskGroup().getTaskGroupId(),
                  schedulingPolicy.getScheduleTimeoutMs()});

          containerManager.getExecutorRepresenterMap().forEach((id, executor) -> {
            Set<String> runningTGIds = executor.getRunningTaskGroups();
            String runningTaskGroups = "";
            for (String tgId : runningTGIds) {
              runningTaskGroups += tgId;
              runningTaskGroups += ", ";
            }
            LOG.info("{}: Running Task Groups: {}", new Object[]{id, runningTaskGroups});
          });

          // Put this TaskGroup back to the queue since we failed to schedule it.
          pendingTaskGroupPriorityQueue.enqueue(nextTaskGroupToSchedule.get());
        } else {
          // Must send this scheduledTaskGroup to the destination executor.
          jobStateManager.onTaskGroupStateChanged(nextTaskGroupToSchedule.get().getTaskGroup(),
              TaskGroupState.State.EXECUTING);
          schedulingPolicy.onTaskGroupScheduled(executorId.get(), nextTaskGroupToSchedule.get());
        }
      } catch (final Exception e) {
        e.printStackTrace(System.err);
        // TODO #285 make SchedulerRunner failure reportable
      }
    }
    if (jobStateManager.getJobState().getStateMachine().getCurrentState() == JobState.State.COMPLETE) {
      LOG.info("Job is complete, scheduler runner will terminate.");
    } else {
      LOG.info("Job is failed, scheduler runner will terminate.");
    }
  }
}
