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
package edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating;

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.edge.executionproperty.DataFlowModelProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.ScheduleGroupIndexProperty;
import org.apache.commons.lang3.mutable.MutableInt;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A pass for assigning each stages in schedule groups.
 * We traverse the DAG topologically to find the dependency information between stages and number them appropriately
 * to give correct order or schedule groups.
 */
public final class ScheduleGroupPass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public ScheduleGroupPass() {
    super(ScheduleGroupIndexProperty.class, Stream.of(
        DataCommunicationPatternProperty.class,
        DataFlowModelProperty.class
    ).collect(Collectors.toSet()));
  }

  /**
   * Determines the range of {@link ScheduleGroupIndexProperty} value that will prevent collision
   * with the existing {@link ScheduleGroupIndexProperty}.
   * @param irVertexCollection collection of {@link IRVertex}
   * @return the minimum value for the {@link ScheduleGroupIndexProperty} that won't collide with the existing values
   */
  private int getNextScheudleGroupIndex(final Collection<IRVertex> irVertexCollection) {
    int nextScheduleGroupIndex = 0;
    for (final IRVertex irVertex : irVertexCollection) {
      final Optional<Integer> scheduleGroupIndex = irVertex.getPropertyValue(ScheduleGroupIndexProperty.class);
      if (scheduleGroupIndex.isPresent()) {
        nextScheduleGroupIndex = Math.max(scheduleGroupIndex.get() + 1, nextScheduleGroupIndex);
      }
    }
    return nextScheduleGroupIndex;
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    final MutableInt nextScheduleGroupIndex = new MutableInt(getNextScheudleGroupIndex(dag.getVertices()));
    dag.topologicalDo(irVertex -> {
      // Base cases for root vertices
      if (!irVertex.getPropertyValue(ScheduleGroupIndexProperty.class).isPresent()) {
        irVertex.getExecutionProperties().put(ScheduleGroupIndexProperty.of(nextScheduleGroupIndex.getValue()));
        nextScheduleGroupIndex.increment();
      }
      // Get scheduleGroupIndex
      final int scheduleGroupIndex = irVertex.getPropertyValue(ScheduleGroupIndexProperty.class).orElseThrow(
          () -> new RuntimeException(String.format("ScheduleGroupIndexProperty must be set for %s", irVertex)));
      // The step case: inductively assign scheduleGroupIndex
      for (final IREdge edge : dag.getOutgoingEdgesOf(irVertex)) {
        final IRVertex connectedIRVertex = edge.getDst();
        // Skip if it already has been assigned scheduleGroupIndex
        if (connectedIRVertex.getPropertyValue(ScheduleGroupIndexProperty.class).isPresent()) {
          continue;
        }
        // Assign scheduleGroupIndex
        if (testMergability(edge)) {
          connectedIRVertex.getExecutionProperties().put(ScheduleGroupIndexProperty.of(scheduleGroupIndex));
        } else {
          connectedIRVertex.getExecutionProperties()
              .put(ScheduleGroupIndexProperty.of(nextScheduleGroupIndex.getValue()));
          nextScheduleGroupIndex.increment();
        }
      }
    });
    return dag;
  }

  /**
   * @param edge {@link IREdge} between two vertices
   * @return {@code true} if and only if the source and destination of the edge can be merged into one ScheduleGroup.
   */
  private boolean testMergability(final IREdge edge) {
    final DataCommunicationPatternProperty.Value pattern = edge.getPropertyValue(DataCommunicationPatternProperty.class)
        .orElseThrow(() -> new RuntimeException(String
            .format("DataCommunicationPatternProperty for %s must be set", edge)));
    final DataFlowModelProperty.Value model = edge.getPropertyValue(DataFlowModelProperty.class)
        .orElseThrow(() -> new RuntimeException(String.format("DataFlowModelProperty for %s must be set", edge)));
    return pattern == DataCommunicationPatternProperty.Value.OneToOne || model == DataFlowModelProperty.Value.Push;
  }
}
