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
import net.jcip.annotations.ThreadSafe;
import org.apache.reef.annotations.audience.DriverSide;

import javax.inject.Inject;
import java.util.Collection;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Keep tracks of all pending task groups.
 */
@ThreadSafe
@DriverSide
public final class PendingTaskGroupQueue {
  private final BlockingDeque<TaskGroup> pendingTaskGroups = new LinkedBlockingDeque<>();

  @Inject
  public PendingTaskGroupQueue() {
  }

  public void addLast(final TaskGroup taskGroup) {
    pendingTaskGroups.addLast(taskGroup);
  }

  public void addAll(final Collection<TaskGroup> taskGroup) {
    pendingTaskGroups.addAll(taskGroup);
  }

  public void addFirst(final TaskGroup taskGroup) {
    pendingTaskGroups.addFirst(taskGroup);
  }

  public TaskGroup takeFirst() throws InterruptedException {
    return pendingTaskGroups.takeFirst();
  }
}
