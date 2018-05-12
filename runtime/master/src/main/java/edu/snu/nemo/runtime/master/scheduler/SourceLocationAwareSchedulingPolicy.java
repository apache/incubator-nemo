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

import edu.snu.nemo.common.ir.Readable;
import edu.snu.nemo.runtime.common.plan.physical.ScheduledTaskGroup;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;

/**
 * This policy is same as {@link RoundRobinSchedulingPolicy}, however for TaskGroups
 * with {@link edu.snu.nemo.common.ir.vertex.SourceVertex}, it tries to pick one of the executors
 * where the corresponding data resides.
 */
@ThreadSafe
@DriverSide
public final class SourceLocationAwareSchedulingPolicy implements SchedulingPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(SourceLocationAwareSchedulingPolicy.class);

  @Inject
  public SourceLocationAwareSchedulingPolicy() {
  }

  /**
   * @param readables collection of readables
   * @return Set of source locations from source tasks in {@code taskGroupDAG}
   * @throws Exception for any exception raised during querying source locations for a readable
   */
  private static Set<String> getSourceLocations(final Collection<Readable> readables) throws Exception {
    final List<String> sourceLocations = new ArrayList<>();
    for (final Readable readable : readables) {
      sourceLocations.addAll(readable.getLocations());
    }
    return new HashSet<>(sourceLocations);
  }

  @Override
  public Set<ExecutorRepresenter> filterExecutorRepresenters(final Set<ExecutorRepresenter> executorRepresenterList,
                                                              final ScheduledTaskGroup scheduledTaskGroup) {
    final Set<String> sourceLocations;
    try {
      sourceLocations = getSourceLocations(scheduledTaskGroup.getLogicalTaskIdToReadable().values());
    } catch (final UnsupportedOperationException e) {
      return executorRepresenterList;
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }

    if (sourceLocations.size() == 0) {
      return executorRepresenterList;
    }

    final Set<ExecutorRepresenter> candidateExecutors =
            executorRepresenterList.stream()
            .filter(executor -> sourceLocations.contains(executor.getNodeName()))
            .collect(Collectors.toSet());

    return candidateExecutors;
  }
}
