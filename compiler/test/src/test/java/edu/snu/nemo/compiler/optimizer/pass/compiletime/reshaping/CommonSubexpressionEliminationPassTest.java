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
package edu.snu.nemo.compiler.optimizer.pass.compiletime.reshaping;

import edu.snu.nemo.client.JobLauncher;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.OperatorVertex;
import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.common.test.EmptyComponents;
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
        .connectVertices(new IREdge(DataCommunicationPatternProperty.Value.OneToOne, source, map1))
        .connectVertices(new IREdge(DataCommunicationPatternProperty.Value.Shuffle, map1, groupByKey))
        .connectVertices(new IREdge(DataCommunicationPatternProperty.Value.OneToOne, groupByKey, combine))
        .connectVertices(new IREdge(DataCommunicationPatternProperty.Value.OneToOne, combine, map2))
        .build();
    dagToOptimize = dagBuilder.addVertex(map1clone).addVertex(groupByKey2).addVertex(combine2).addVertex(map22)
        .connectVertices(new IREdge(DataCommunicationPatternProperty.Value.OneToOne, source, map1clone))
        .connectVertices(new IREdge(DataCommunicationPatternProperty.Value.Shuffle, map1clone, groupByKey2))
        .connectVertices(new IREdge(DataCommunicationPatternProperty.Value.OneToOne, groupByKey2, combine2))
        .connectVertices(new IREdge(DataCommunicationPatternProperty.Value.OneToOne, combine2, map22))
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
