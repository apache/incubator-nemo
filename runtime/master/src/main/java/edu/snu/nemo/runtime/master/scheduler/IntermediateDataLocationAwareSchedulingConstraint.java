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

import edu.snu.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import edu.snu.nemo.common.ir.executionproperty.AssociatedProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.IntermediateDataLocationAwareSchedulingProperty;
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
 * A scheduling constraint that tries to pick the executor where the intermediate data to read reside.
 */
@ThreadSafe
@DriverSide
@AssociatedProperty(IntermediateDataLocationAwareSchedulingProperty.class)
public final class IntermediateDataLocationAwareSchedulingConstraint implements SchedulingConstraint {
  private final BlockManagerMaster blockManagerMaster;

  @Inject
  private IntermediateDataLocationAwareSchedulingConstraint(final BlockManagerMaster blockManagerMaster) {
    this.blockManagerMaster = blockManagerMaster;
  }

  /**
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
        if (locationHandler.getLocationFuture().isDone()) {
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

  @Override
  public boolean testSchedulability(final ExecutorRepresenter executor, final Task task) {
    final Optional<String> optionalIntermediateLoc = getIntermediateDataLocation(task);

    if (getIntermediateDataLocation(task).isPresent()) {
      return optionalIntermediateLoc.get().equals(executor.getExecutorId());
    } else {
      return true;
    }
  }
}
