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
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;

/**
 * This policy chooses a set of Executors, on which have minimum running Tasks.
 */
@ThreadSafe
@DriverSide
public final class MinOccupancyFirstSchedulingPolicy implements SchedulingPolicy {

  @Inject
  private MinOccupancyFirstSchedulingPolicy() {
  }

  @Override
  public ExecutorRepresenter selectExecutor(final Collection<ExecutorRepresenter> executors, final Task task) {
    final OptionalInt minOccupancy =
        executors.stream()
        .map(executor -> executor.getNumOfRunningTasks())
        .mapToInt(i -> i).min();

    if (!minOccupancy.isPresent()) {
      throw new RuntimeException("Cannot find min occupancy");
    }

    return executors.stream()
        .filter(executor -> executor.getNumOfRunningTasks() == minOccupancy.getAsInt())
        .findFirst()
        .orElseThrow(() -> new RuntimeException("No such executor"));
  }
}
