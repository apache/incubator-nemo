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
import edu.snu.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import edu.snu.nemo.common.ir.executionproperty.AssociatedProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.ResourceLocalityProperty;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.plan.StageEdge;
import edu.snu.nemo.runtime.common.plan.Task;
import edu.snu.nemo.runtime.master.BlockManagerMaster;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * This policy tries to pick the executors where the corresponding source or intermediate data for a task reside.
 */
@ThreadSafe
@DriverSide
@AssociatedProperty(ResourceLocalityProperty.class)
public final class SourceLocationAwareSchedulingConstraint implements SchedulingConstraint {
  private final BlockManagerMaster blockManagerMaster;

  @Inject
  private SourceLocationAwareSchedulingConstraint(final BlockManagerMaster blockManagerMaster) {
    this.blockManagerMaster = blockManagerMaster;
  }

  /**
   * Find the location of the intermediate data for a task.
   * It is only possible if the task receives only one input edge with One-to-One communication pattern, and
   * the location of the input data is known.
   *
   * @param task the task to schedule.
   * @return the intermediate data location.
   */
  private Optional<String> getIntermediateDataLocation(final Task task) {
    if (task.getTaskIncomingEdges().size() == 1) {
      final StageEdge physicalStageEdge = task.getTaskIncomingEdges().get(0);
      if (CommunicationPatternProperty.Value.OneToOne.equals(
          physicalStageEdge.getPropertyValue(CommunicationPatternProperty.class)
              .orElseThrow(() -> new RuntimeException("No comm pattern!")))) {
        final String blockIdToRead =
            RuntimeIdGenerator.generateBlockId(physicalStageEdge.getId(),
                RuntimeIdGenerator.getIndexFromTaskId(task.getTaskId()));
        final BlockManagerMaster.BlockLocationRequestHandler locationHandler =
            blockManagerMaster.getBlockLocationHandler(blockIdToRead);
        if (locationHandler.getLocationFuture().isDone()) { // if the location is known.
          try {
            final String location = locationHandler.getLocationFuture().get();
            return Optional.of(location);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          } catch (final ExecutionException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }
    return Optional.empty();
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

  @Override
  public boolean testSchedulability(final ExecutorRepresenter executor, final Task task) {
    if (task.getTaskIncomingEdges().isEmpty()) { // Source task
      final Set<String> sourceLocations;
      try {
        sourceLocations = getSourceLocations(task.getIrVertexIdToReadable().values());
      } catch (final UnsupportedOperationException e) {
        return true;
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }

      if (sourceLocations.size() == 0) {
        return true;
      }

      return sourceLocations.contains(executor.getNodeName());
    } else { // Non-source task.
      final Optional<String> optionalIntermediateLoc = getIntermediateDataLocation(task);

      if (getIntermediateDataLocation(task).isPresent()) {
        return optionalIntermediateLoc.get().equals(executor.getExecutorId());
      } else {
        return true;
      }
    }
  }
}
