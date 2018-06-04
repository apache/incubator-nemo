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
import edu.snu.nemo.common.ir.Readable;
import edu.snu.nemo.runtime.common.plan.ExecutableTask;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;

/**
 * This policy is same as {@link RoundRobinSchedulingPolicy}, however for Tasks
 * with {@link edu.snu.nemo.common.ir.vertex.SourceVertex}, it tries to pick one of the executors
 * where the corresponding data resides.
 */
@ThreadSafe
@DriverSide
public final class SourceLocationAwareSchedulingPolicy implements SchedulingPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(SourceLocationAwareSchedulingPolicy.class);

  @VisibleForTesting
  @Inject
  public SourceLocationAwareSchedulingPolicy() {
  }

  /**
   * @param readables collection of readables
   * @return Set of source locations from source tasks in {@code taskDAG}
   * @throws Exception for any exception raised during querying source locations for a readable
   */
  private static Set<String> getSourceLocations(final Collection<Readable> readables) throws Exception {
    final List<String> sourceLocations = new ArrayList<>();
    for (final Readable readable : readables) {
      sourceLocations.addAll(readable.getLocations());
    }
    return new HashSet<>(sourceLocations);
  }

  /**
   * @param executorRepresenterSet Set of {@link ExecutorRepresenter} to be filtered by source location.
   *                               If there is no source locations, will return original set.
   * @param executableTask {@link ExecutableTask} to be scheduled.
   * @return filtered Set of {@link ExecutorRepresenter}.
   */
  @Override
  public Set<ExecutorRepresenter> filterExecutorRepresenters(final Set<ExecutorRepresenter> executorRepresenterSet,
                                                             final ExecutableTask executableTask) {
    final Set<String> sourceLocations;
    try {
      sourceLocations = getSourceLocations(executableTask.getIrVertexIdToReadable().values());
    } catch (final UnsupportedOperationException e) {
      return executorRepresenterSet;
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }

    if (sourceLocations.size() == 0) {
      return executorRepresenterSet;
    }

    final Set<ExecutorRepresenter> candidateExecutors =
            executorRepresenterSet.stream()
            .filter(executor -> sourceLocations.contains(executor.getNodeName()))
            .collect(Collectors.toSet());

    return candidateExecutors;
  }
}
