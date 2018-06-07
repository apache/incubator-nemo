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
import edu.snu.nemo.common.ir.vertex.executionproperty.ExecutorPlacementProperty;
import edu.snu.nemo.runtime.common.plan.Task;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;

import javax.inject.Inject;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This policy find executors which has corresponding container type.
 */
public final class ContainerTypeAwareSchedulingPolicy implements SchedulingPolicy {

  @VisibleForTesting
  @Inject
  public ContainerTypeAwareSchedulingPolicy() {
  }

  /**
   * @param executorRepresenterSet Set of {@link ExecutorRepresenter} to be filtered by the container type.
   *                               If the container type of target Task is NONE, it will return the original set.
   * @param task {@link Task} to be scheduled.
   * @return filtered Set of {@link ExecutorRepresenter}.
   */
  @Override
  public Set<ExecutorRepresenter> filterExecutorRepresenters(final Set<ExecutorRepresenter> executorRepresenterSet,
                                                             final Task task) {

    if (task.getContainerType().equals(ExecutorPlacementProperty.NONE)) {
      return executorRepresenterSet;
    }

    final Set<ExecutorRepresenter> candidateExecutors =
        executorRepresenterSet.stream()
            .filter(executor -> executor.getContainerType().equals(task.getContainerType()))
            .collect(Collectors.toSet());

    return candidateExecutors;
  }
}
