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
import edu.snu.vortex.runtime.master.ExecutorRepresenter;

import java.util.Optional;

// TODO #93: Implement Batch Scheduler
/**
 * {@inheritDoc}
 * A Batch implementation.
 */
public final class BatchScheduler implements SchedulingPolicy {

  public BatchScheduler() {

  }

  @Override
  public long getScheduleTimeout() {
    return 0;
  }

  @Override
  public Optional<String> attemptSchedule(final TaskGroup taskGroup) {
    return null;
  }

  @Override
  public void onExecutorAdded(final ExecutorRepresenter executor) {

  }

  @Override
  public void onExecutorDeleted(final ExecutorRepresenter executor) {

  }

  @Override
  public void onTaskGroupScheduled(final ExecutorRepresenter executor, final TaskGroup taskGroup) {

  }

  @Override
  public void onTaskGroupLaunched(final ExecutorRepresenter executor, final TaskGroup taskGroup) {

  }

  @Override
  public void onTaskGroupExecutionComplete(final ExecutorRepresenter executor, final TaskGroup taskGroup) {

  }
}
