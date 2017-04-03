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
import edu.snu.vortex.runtime.common.plan.physical.PhysicalPlan;
import edu.snu.vortex.runtime.common.plan.physical.TaskGroup;
import edu.snu.vortex.runtime.exception.SchedulingException;
import edu.snu.vortex.runtime.master.ExecutorRepresenter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Scheduler receives a {@link PhysicalPlan} to execute and asynchronously schedules the task groups.
 * The policy by which it schedules them is dependent on the implementation of {@link SchedulingPolicy}.
 */
public final class Scheduler {
  private static final Logger LOG = Logger.getLogger(Scheduler.class.getName());

  private final ExecutorService schedulerThread;
  private final BlockingDeque<TaskGroup> taskGroupsToSchedule;
  private final Map<String, ExecutorRepresenter> executorRepresenterMap;
  private SchedulingPolicy schedulingPolicy;
  private List<List<TaskGroup>> taskGroupsByStage;

  public Scheduler(final RuntimeAttribute schedulingPolicy) {
    this.schedulerThread = Executors.newSingleThreadExecutor();
    this.taskGroupsToSchedule = new LinkedBlockingDeque<>();
    this.executorRepresenterMap = new HashMap<>();
    schedulerThread.execute(new TaskGroupScheduleHandler());

    // The default policy is initialized and set here.
    setSchedulingPolicy(schedulingPolicy);
  }

  /**
   * Receives a job to schedule.
   * @param physicalPlan the physical plan for the job.
   */
  public void scheduleJob(final PhysicalPlan physicalPlan) {
    this.taskGroupsByStage = physicalPlan.getTaskGroupsByStage();
  }

  // TODO #90: Integrate Components for Single-Machine End-to-End Execution
  public void onTaskGroupExecutionComplete(final ExecutorRepresenter executor, final TaskGroup taskGroup) {
    schedulingPolicy.onTaskGroupExecutionComplete(executor, taskGroup);
    scheduleNextStage();
  }

  /**
   * Schedules the next stage to execute.
   * It takes the list for task groups for the stage and adds them where the scheduler thread continuously polls from.
   */
  private void scheduleNextStage() {
    final List<TaskGroup> taskGroupList = taskGroupsByStage.remove(0);
    taskGroupsToSchedule.addAll(taskGroupList);
  }

  /**
   * Sets the scheduling policy.
   * This can be called anytime during this scheduler's lifetime and the policy will change flexibly.
   * @param schedulingPolicy The attribute for the scheduling policy.
   */
  private void setSchedulingPolicy(final RuntimeAttribute schedulingPolicy) {
    switch (schedulingPolicy) {
    case Batch:
      this.schedulingPolicy = new BatchScheduler();
      break;
    case SamplePolicy:
      this.schedulingPolicy = SampleScheduler.newInstance();
      break;
    default:
      throw new SchedulingException("The scheduling policy is unsupported by runtime");
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
          final Optional<String> executorId = schedulingPolicy.attemptSchedule(taskGroup);
          if (!executorId.isPresent()) {
            LOG.log(Level.INFO, "Failed to assign an executor before the timeout: {0}",
                schedulingPolicy.getScheduleTimeout());
            taskGroupsToSchedule.addLast(taskGroup);
          } else {
            // TODO #90: Integrate Components for Single-Machine End-to-End Execution
            // Must send this taskGroup to the destination executor.
            schedulingPolicy.onTaskGroupScheduled(executorRepresenterMap.get(executorId.get()), taskGroup);
          }
        } catch (final Exception e) {
          throw new SchedulingException(e.getMessage());
        }
      }
    }
  }

  public void terminate() {
    schedulerThread.shutdown();
    taskGroupsToSchedule.clear();
  }
}
