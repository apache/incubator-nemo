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

import com.google.common.annotations.VisibleForTesting;
import edu.snu.nemo.runtime.common.plan.physical.ScheduledTaskGroup;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;

import javax.inject.Inject;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This policy finds executor that has free slot for a TaskGroup.
 */
public final class FreeSlotSchedulingPolicy implements SchedulingPolicy {
  @VisibleForTesting
  @Inject
  public FreeSlotSchedulingPolicy() {
  }

  /**
   * @param executorRepresenterSet Set of {@link ExecutorRepresenter} to be filtered by the free slot of executors.
   *                               Executors that do not have any free slots will be filtered by this policy.
   * @param scheduledTaskGroup {@link ScheduledTaskGroup} to be scheduled.
   * @return filtered Set of {@link ExecutorRepresenter}.
   */
  @Override
  public Set<ExecutorRepresenter> filterExecutorRepresenters(final Set<ExecutorRepresenter> executorRepresenterSet,
                                                             final ScheduledTaskGroup scheduledTaskGroup) {
    final Set<ExecutorRepresenter> candidateExecutors =
        executorRepresenterSet.stream()
            .filter(executor -> executor.getRunningTaskGroups().size() < executor.getExecutorCapacity())
            .collect(Collectors.toSet());

    return candidateExecutors;
  }
}
