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
import edu.snu.nemo.runtime.common.plan.Task;
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
 * The RR policy is used for each container type when trying to schedule a task.
 */
@ThreadSafe
@DriverSide
public final class RoundRobinSchedulingPolicy implements SchedulingPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(RoundRobinSchedulingPolicy.class.getName());

  @VisibleForTesting
  @Inject
  public RoundRobinSchedulingPolicy() {
  }

  /**
   * @param executorRepresenterSet Set of {@link ExecutorRepresenter} to be filtered by round robin behaviour.
   * @param task {@link Task} to be scheduled.
   * @return filtered Set of {@link ExecutorRepresenter}.
   */
  @Override
  public Set<ExecutorRepresenter> filterExecutorRepresenters(final Set<ExecutorRepresenter> executorRepresenterSet,
                                                             final Task task) {
    final OptionalInt minOccupancy =
        executorRepresenterSet.stream()
        .map(executor -> executor.getRunningTasks().size())
        .mapToInt(i -> i).min();

    if (!minOccupancy.isPresent()) {
      return Collections.emptySet();
    }

    final Set<ExecutorRepresenter> candidateExecutors =
        executorRepresenterSet.stream()
        .filter(executor -> executor.getRunningTasks().size() == minOccupancy.getAsInt())
        .collect(Collectors.toSet());

    return candidateExecutors;
  }
}
