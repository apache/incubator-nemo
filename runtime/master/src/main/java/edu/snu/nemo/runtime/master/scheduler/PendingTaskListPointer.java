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
import java.util.List;
import java.util.Optional;

/**
 * Points to a list of pending tasks eligible for scheduling.
 */
@ThreadSafe
public class PendingTaskListPointer {
  List<Task> tasks;

  @Inject
  public PendingTaskListPointer() {
  }


  synchronized void set(final List<Task> tasks) {
    this.tasks = tasks;
  }

  synchronized void setIfNull(final List<Task> tasks) {
    if (this.tasks == null) {
      this.tasks = tasks;
    }
  }

  synchronized Optional<List<Task>> getAndSetNull() {
    final List<Task> cur = tasks;
    tasks = null;
    return Optional.ofNullable(cur);
  }
}
