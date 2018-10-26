package org.apache.nemo.compiler.optimizer.pass.compiletime.reshaping;

import org.apache.nemo.client.JobLauncher;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.test.EmptyComponents;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;

/**
 * Tes {@link CommonSubexpressionEliminationPass}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public class CommonSubexpressionEliminationPassTest {
  private final IRVertex source = new EmptyComponents.EmptySourceVertex<>("Source");
  private final IRVertex map1 = new OperatorVertex(new EmptyComponents.EmptyTransform("MapElements"));
  private final IRVertex groupByKey = new OperatorVertex(new EmptyComponents.EmptyTransform("GroupByKey"));
  private final IRVertex combine = new OperatorVertex(new EmptyComponents.EmptyTransform("Combine"));
  private final IRVertex map2 = new OperatorVertex(new EmptyComponents.EmptyTransform("MapElements2"));

  private final IRVertex map1clone = map1.getClone();
  private final IRVertex groupByKey2 = new OperatorVertex(new EmptyComponents.EmptyTransform("GroupByKey2"));
  private final IRVertex combine2 = new OperatorVertex(new EmptyComponents.EmptyTransform("Combine2"));
  private final IRVertex map22 = new OperatorVertex(new EmptyComponents.EmptyTransform("Map2"));

  private DAG<IRVertex, IREdge> dagNotToOptimize;
  private DAG<IRVertex, IREdge> dagToOptimize;

  @Before
  public void setUp() {
    final DAGBuilder<IRVertex, IREdge> dagBuilder = new DAGBuilder<>();
    dagNotToOptimize = dagBuilder.addVertex(source).addVertex(map1).addVertex(groupByKey).addVertex(combine)
        .addVertex(map2)
        .connectVertices(new IREdge(CommunicationPatternProperty.Value.OneToOne, source, map1))
        .connectVertices(new IREdge(CommunicationPatternProperty.Value.Shuffle, map1, groupByKey))
        .connectVertices(new IREdge(CommunicationPatternProperty.Value.OneToOne, groupByKey, combine))
        .connectVertices(new IREdge(CommunicationPatternProperty.Value.OneToOne, combine, map2))
        .build();
    dagToOptimize = dagBuilder.addVertex(map1clone).addVertex(groupByKey2).addVertex(combine2).addVertex(map22)
        .connectVertices(new IREdge(CommunicationPatternProperty.Value.OneToOne, source, map1clone))
        .connectVertices(new IREdge(CommunicationPatternProperty.Value.Shuffle, map1clone, groupByKey2))
        .connectVertices(new IREdge(CommunicationPatternProperty.Value.OneToOne, groupByKey2, combine2))
        .connectVertices(new IREdge(CommunicationPatternProperty.Value.OneToOne, combine2, map22))
        .build();
  }

  @Test
  public void testCommonSubexpressionEliminationPass() {
    final long originalVerticesNum = dagNotToOptimize.getVertices().size();
    final long optimizedVerticesNum = dagToOptimize.getVertices().size();

    final DAG<IRVertex, IREdge> processedDAG = new CommonSubexpressionEliminationPass().apply(dagToOptimize);
    assertEquals(optimizedVerticesNum - 1, processedDAG.getVertices().size());

    final DAG<IRVertex, IREdge> notProcessedDAG = new CommonSubexpressionEliminationPass().apply(dagNotToOptimize);
    assertEquals(originalVerticesNum, notProcessedDAG.getVertices().size());
  }
}
