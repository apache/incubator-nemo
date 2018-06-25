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

import edu.snu.nemo.runtime.common.plan.Task;

import net.jcip.annotations.ThreadSafe;

import javax.inject.Inject;
import java.util.Collection;
import java.util.Optional;

/**
 * Points to a collection of pending tasks eligible for scheduling.
 * This collection of tasks is a subset of a scheduling group, and can be scheduled in any order.
 */
@ThreadSafe
public class PendingTaskCollectionPointer {
  private Collection<Task> tasks;

  @Inject
  public PendingTaskCollectionPointer() {
  }

  /**
   * This collection of tasks should take precedence over any previous collection of tasks.
   * @param tasks to schedule.
   */
  synchronized void setToOverwrite(final Collection<Task> tasks) {
    this.tasks = tasks;
  }

  /**
   * This collection of tasks can be scheduled only if there's no collection of tasks to schedule at the moment.
   * @param tasks to schedule
   */
  synchronized void setIfNull(final Collection<Task> tasks) {
    if (this.tasks == null) {
      this.tasks = tasks;
    }
  }

  /**
   * Take the whole collection of tasks to schedule, and set the pointer to null.
   * @return optional tasks to schedule
   */
  synchronized Optional<Collection<Task>> getAndSetNull() {
    final Collection<Task> cur = tasks;
    tasks = null;
    return Optional.ofNullable(cur);
  }
}
