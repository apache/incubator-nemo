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
package edu.snu.nemo.runtime.common.plan;

import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.nemo.common.ir.executionproperty.VertexExecutionProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.OperatorVertex;
import edu.snu.nemo.common.ir.vertex.executionproperty.DynamicOptimizationProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.ExecutorPlacementProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.ScheduleGroupProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.AdditionalTagOutputProperty;
import edu.snu.nemo.common.ir.vertex.transform.Transform;
import edu.snu.nemo.common.test.EmptyComponents;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static edu.snu.nemo.common.test.EmptyComponents.EMPTY_TRANSFORM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Tests {@link StagePartitioner}.
 */
public final class StagePartitionerTest {
  private StagePartitioner stagePartitioner;

  @Before
  public void setup() throws InjectionException {
    stagePartitioner = Tang.Factory.getTang().newInjector().getInstance(StagePartitioner.class);
    stagePartitioner.addIgnoredPropertyKey(DynamicOptimizationProperty.class);
    stagePartitioner.addIgnoredPropertyKey(AdditionalTagOutputProperty.class);
  }

  /**
   * @param parallelism {@link ParallelismProperty} value for the new vertex
   * @param scheduleGroup {@link ScheduleGroupProperty} value for the new vertex
   * @param otherProperties other {@link VertexExecutionProperty} for the new vertex
   * @return new {@link IRVertex}
   */
  private static IRVertex newVertex(final int parallelism, final int scheduleGroup,
                                    final List<VertexExecutionProperty> otherProperties) {
    final IRVertex vertex = new OperatorVertex(EMPTY_TRANSFORM);
    vertex.getExecutionProperties().put(ParallelismProperty.of(parallelism));
    vertex.getExecutionProperties().put(ScheduleGroupProperty.of(scheduleGroup));
    otherProperties.forEach(property -> vertex.getExecutionProperties().put(property));
    return vertex;
  }

  /**
   * A simple case where two vertices have common parallelism and ScheduleGroup so that get merged into one stage.
   */
  @Test
  public void testLinear() {
    final DAGBuilder<IRVertex, IREdge> dagBuilder = new DAGBuilder<>();
    final IRVertex v0 = newVertex(5, 0, Collections.emptyList());
    final IRVertex v1 = newVertex(5, 0, Collections.emptyList());
    dagBuilder.addVertex(v0);
    dagBuilder.addVertex(v1);
    dagBuilder.connectVertices(new IREdge(DataCommunicationPatternProperty.Value.OneToOne, v0, v1));
    final Map<IRVertex, Integer> partitioning = stagePartitioner.apply(dagBuilder.buildWithoutSourceSinkCheck());
    assertEquals(0, (int) partitioning.get(v0));
    assertEquals(0, (int) partitioning.get(v1));
  }

  /**
   * A simple case where two vertices have different parallelism.
   */
  @Test
  public void testSplitByParallelism() {
    final DAGBuilder<IRVertex, IREdge> dagBuilder = new DAGBuilder<>();
    final IRVertex v0 = newVertex(5, 0, Collections.emptyList());
    final IRVertex v1 = newVertex(1, 0, Collections.emptyList());
    dagBuilder.addVertex(v0);
    dagBuilder.addVertex(v1);
    dagBuilder.connectVertices(new IREdge(DataCommunicationPatternProperty.Value.OneToOne, v0, v1));
    final Map<IRVertex, Integer> partitioning = stagePartitioner.apply(dagBuilder.buildWithoutSourceSinkCheck());
    assertNotEquals(partitioning.get(v0), partitioning.get(v1));
  }

  /**
   * A simple case where two vertices have different ScheduleGroup.
   */
  @Test
  public void testSplitByScheduleGroup() {
    final DAGBuilder<IRVertex, IREdge> dagBuilder = new DAGBuilder<>();
    final IRVertex v0 = newVertex(1, 0, Collections.emptyList());
    final IRVertex v1 = newVertex(1, 1, Collections.emptyList());
    dagBuilder.addVertex(v0);
    dagBuilder.addVertex(v1);
    dagBuilder.connectVertices(new IREdge(DataCommunicationPatternProperty.Value.OneToOne, v0, v1));
    final Map<IRVertex, Integer> partitioning = stagePartitioner.apply(dagBuilder.buildWithoutSourceSinkCheck());
    assertNotEquals(partitioning.get(v0), partitioning.get(v1));
  }

  /**
   * A simple case where two vertices are connected with Shuffle edge.
   */
  @Test
  public void testSplitByShuffle() {
    final DAGBuilder<IRVertex, IREdge> dagBuilder = new DAGBuilder<>();
    final IRVertex v0 = newVertex(1, 0, Collections.emptyList());
    final IRVertex v1 = newVertex(1, 0, Collections.emptyList());
    dagBuilder.addVertex(v0);
    dagBuilder.addVertex(v1);
    dagBuilder.connectVertices(new IREdge(DataCommunicationPatternProperty.Value.Shuffle, v0, v1));
    final Map<IRVertex, Integer> partitioning = stagePartitioner.apply(dagBuilder.buildWithoutSourceSinkCheck());
    assertNotEquals(partitioning.get(v0), partitioning.get(v1));
  }

  /**
   * A simple case where one of the two vertices has additional property.
   */
  @Test
  public void testSplitByOtherProperty() {
    final DAGBuilder<IRVertex, IREdge> dagBuilder = new DAGBuilder<>();
    final IRVertex v0 = newVertex(1, 0,
        Arrays.asList(ExecutorPlacementProperty.of(ExecutorPlacementProperty.RESERVED)));
    final IRVertex v1 = newVertex(1, 0, Collections.emptyList());
    dagBuilder.addVertex(v0);
    dagBuilder.addVertex(v1);
    dagBuilder.connectVertices(new IREdge(DataCommunicationPatternProperty.Value.OneToOne, v0, v1));
    final Map<IRVertex, Integer> partitioning = stagePartitioner.apply(dagBuilder.buildWithoutSourceSinkCheck());
    assertNotEquals(partitioning.get(v0), partitioning.get(v1));
  }

  /**
   * A simple case where one of the two vertices has ignored property.
   */
  @Test
  public void testNotSplitByIgnoredProperty() {
    final DAGBuilder<IRVertex, IREdge> dagBuilder = new DAGBuilder<>();
    final IRVertex v0 = newVertex(1, 0,
        Arrays.asList(DynamicOptimizationProperty.of(DynamicOptimizationProperty.Value.DataSkewRuntimePass)));
    final IRVertex v1 = newVertex(1, 0, Collections.emptyList());
    dagBuilder.addVertex(v0);
    dagBuilder.addVertex(v1);
    dagBuilder.connectVertices(new IREdge(DataCommunicationPatternProperty.Value.OneToOne, v0, v1));
    final Map<IRVertex, Integer> partitioning = stagePartitioner.apply(dagBuilder.buildWithoutSourceSinkCheck());
    assertEquals(0, (int) partitioning.get(v0));
    assertEquals(0, (int) partitioning.get(v1));
  }

  /**
   * Test scenario when there is a join.
   */
  @Test
  public void testJoin() {
    final DAGBuilder<IRVertex, IREdge> dagBuilder = new DAGBuilder<>();
    final IRVertex v0 = newVertex(5, 0, Collections.emptyList());
    final IRVertex v1 = newVertex(5, 0, Collections.emptyList());
    final IRVertex v2 = newVertex(5, 0, Collections.emptyList());
    dagBuilder.addVertex(v0);
    dagBuilder.addVertex(v1);
    dagBuilder.addVertex(v2);
    dagBuilder.connectVertices(new IREdge(DataCommunicationPatternProperty.Value.OneToOne, v0, v2));
    dagBuilder.connectVertices(new IREdge(DataCommunicationPatternProperty.Value.OneToOne, v1, v2));
    final Map<IRVertex, Integer> partitioning = stagePartitioner.apply(dagBuilder.buildWithoutSourceSinkCheck());
    assertNotEquals(partitioning.get(v0), partitioning.get(v1));
    assertNotEquals(partitioning.get(v1), partitioning.get(v2));
    assertNotEquals(partitioning.get(v2), partitioning.get(v0));
  }
}
