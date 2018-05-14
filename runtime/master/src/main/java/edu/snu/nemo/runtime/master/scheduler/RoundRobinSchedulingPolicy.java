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

import edu.snu.nemo.runtime.common.plan.physical.ScheduledTaskGroup;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;

/**
 * {@inheritDoc}
 * A Round-Robin implementation used by {@link BatchSingleJobScheduler}.
 *
 * This policy keeps a list of available {@link ExecutorRepresenter} for each type of container.
 * The RR policy is used for each container type when trying to schedule a task group.
 */
@ThreadSafe
@DriverSide
public final class RoundRobinSchedulingPolicy implements SchedulingPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(RoundRobinSchedulingPolicy.class.getName());

  @Inject
  public RoundRobinSchedulingPolicy() {
  }

  @Override
  public Set<ExecutorRepresenter> filterExecutorRepresenters(final Set<ExecutorRepresenter> executorRepresenterList,
                                                             final ScheduledTaskGroup scheduledTaskGroup) {
    final OptionalInt minOccupancy =
        executorRepresenterList.stream()
        .map(executor -> executor.getRunningTaskGroups().size())
        .mapToInt(i -> i).min();

    if (!minOccupancy.isPresent()) {
      return Collections.emptySet();
    }

    final Set<ExecutorRepresenter> candidateExecutors =
        executorRepresenterList.stream()
        .filter(executor -> executor.getRunningTaskGroups().size() == minOccupancy.getAsInt())
        .collect(Collectors.toSet());

    return candidateExecutors;
  }
}
