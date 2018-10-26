package org.apache.nemo.runtime.common.plan;

import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.*;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.nemo.common.test.EmptyComponents.EMPTY_TRANSFORM;
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
    vertex.setProperty(ParallelismProperty.of(parallelism));
    vertex.setProperty(ScheduleGroupProperty.of(scheduleGroup));
    otherProperties.forEach(property -> vertex.setProperty(property));
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
    dagBuilder.connectVertices(new IREdge(CommunicationPatternProperty.Value.OneToOne, v0, v1));
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
    dagBuilder.connectVertices(new IREdge(CommunicationPatternProperty.Value.OneToOne, v0, v1));
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
    dagBuilder.connectVertices(new IREdge(CommunicationPatternProperty.Value.OneToOne, v0, v1));
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
    dagBuilder.connectVertices(new IREdge(CommunicationPatternProperty.Value.Shuffle, v0, v1));
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
        Arrays.asList(ResourcePriorityProperty.of(ResourcePriorityProperty.RESERVED)));
    final IRVertex v1 = newVertex(1, 0, Collections.emptyList());
    dagBuilder.addVertex(v0);
    dagBuilder.addVertex(v1);
    dagBuilder.connectVertices(new IREdge(CommunicationPatternProperty.Value.OneToOne, v0, v1));
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
    dagBuilder.connectVertices(new IREdge(CommunicationPatternProperty.Value.OneToOne, v0, v1));
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
    dagBuilder.connectVertices(new IREdge(CommunicationPatternProperty.Value.OneToOne, v0, v2));
    dagBuilder.connectVertices(new IREdge(CommunicationPatternProperty.Value.OneToOne, v1, v2));
    final Map<IRVertex, Integer> partitioning = stagePartitioner.apply(dagBuilder.buildWithoutSourceSinkCheck());
    assertNotEquals(partitioning.get(v0), partitioning.get(v1));
    assertNotEquals(partitioning.get(v1), partitioning.get(v2));
    assertNotEquals(partitioning.get(v2), partitioning.get(v0));
  }
}
