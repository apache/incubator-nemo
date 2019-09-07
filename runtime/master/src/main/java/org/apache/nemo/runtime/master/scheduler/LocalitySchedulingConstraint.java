/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.runtime.master.scheduler;

import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DuplicateEdgeGroupProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DuplicateEdgeGroupPropertyValue;
import org.apache.nemo.common.ir.executionproperty.AssociatedProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourceLocalityProperty;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.common.state.BlockState;
import org.apache.nemo.runtime.master.BlockManagerMaster;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * This policy tries to pick the executors where the corresponding source or intermediate data for a task reside.
 */
@ThreadSafe
@DriverSide
@AssociatedProperty(ResourceLocalityProperty.class)
public final class LocalitySchedulingConstraint implements SchedulingConstraint {
  private final BlockManagerMaster blockManagerMaster;

  @Inject
  private LocalitySchedulingConstraint(final BlockManagerMaster blockManagerMaster) {
    this.blockManagerMaster = blockManagerMaster;
  }

  /**
   * Find the locations of the intermediate data for a task.
   * It is only possible if the task receives only one input edge with One-to-One communication pattern, and
   * the location of the input data is known.
   *
   * @param task the task to schedule.
   * @return the intermediate data locations, empty if none exists.
   */
  private List<String> getIntermediateDataLocations(final Task task) {
    if (task.getTaskIncomingEdges().size() == 1) {
      final StageEdge physicalStageEdge = task.getTaskIncomingEdges().get(0);
      if (CommunicationPatternProperty.Value.ONE_TO_ONE.equals(
        physicalStageEdge.getPropertyValue(CommunicationPatternProperty.class)
          .orElseThrow(() -> new RuntimeException("No comm pattern!")))) {
        final Optional<DuplicateEdgeGroupPropertyValue> dupProp =
          physicalStageEdge.getPropertyValue(DuplicateEdgeGroupProperty.class);
        final String representativeEdgeId = dupProp.isPresent()
          ? dupProp.get().getRepresentativeEdgeId()
          : physicalStageEdge.getId();

        final String blockIdToRead = RuntimeIdManager.generateBlockId(representativeEdgeId, task.getTaskId());
        return blockManagerMaster.getBlockHandlers(blockIdToRead, BlockState.State.AVAILABLE)
          .stream()
          .map(handler -> {
            try {
              return handler.getLocationFuture().get();
            } catch (final ExecutionException e) {
              throw new RuntimeException(e);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new RuntimeException(e);
            }
          })
          .collect(Collectors.toList());
      }
    }
    return Collections.emptyList();
  }

  /**
   * @param readables collection of readables
   * @return Set of source locations from source tasks in {@code taskDAG}
   * @throws Exception for any exception raised during querying source locations for a readable
   */
  private static Set<String> getSourceDataLocations(final Collection<Readable> readables) throws Exception {
    final List<String> sourceLocations = new ArrayList<>();
    for (final Readable readable : readables) {
      sourceLocations.addAll(readable.getLocations());
    }
    return new HashSet<>(sourceLocations);
  }

  @Override
  public boolean testSchedulability(final ExecutorRepresenter executor, final Task task) {
    if (task.getTaskIncomingEdges().isEmpty()) {
      // Source task
      final Set<String> sourceLocations;
      try {
        sourceLocations = getSourceDataLocations(task.getIrVertexIdToReadable().values());
      } catch (final UnsupportedOperationException e) {
        return true;
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }

      if (sourceLocations.size() == 0) {
        return true;
      }

      return sourceLocations.contains(executor.getNodeName());
    } else {
      // Non-source task.
      final List<String> intermediateLocations = getIntermediateDataLocations(task);
      if (intermediateLocations.isEmpty()) {
        // Since there is no known location, we just schedule the task to any executor.
        return true;
      } else {
        // There is a known location(s), so we schedule to it(them).
        return intermediateLocations.contains(executor.getExecutorId());
      }
    }
  }
}
