/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.onyx.tests.compiler.optimizer.pass.compiletime.composite;

import edu.snu.onyx.client.JobLauncher;
import edu.snu.onyx.common.dag.DAG;
import edu.snu.onyx.common.ir.edge.IREdge;
import edu.snu.onyx.common.ir.edge.executionproperty.DataFlowModelProperty;
import edu.snu.onyx.common.ir.edge.executionproperty.DataStoreProperty;
import edu.snu.onyx.common.ir.executionproperty.ExecutionProperty;
import edu.snu.onyx.common.ir.vertex.IRVertex;
import edu.snu.onyx.common.ir.vertex.executionproperty.ExecutorPlacementProperty;
import edu.snu.onyx.compiler.optimizer.pass.compiletime.annotating.PadoEdgeDataStorePass;
import edu.snu.onyx.compiler.optimizer.pass.compiletime.annotating.PadoVertexExecutorPlacementPass;
import edu.snu.onyx.compiler.optimizer.pass.compiletime.composite.PadoCompositePass;
import edu.snu.onyx.tests.compiler.CompilerTestUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link PadoVertexExecutorPlacementPass} and {@link PadoEdgeDataStorePass}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public class PadoCompositePassTest {
  private DAG<IRVertex, IREdge> compiledDAG;

  @Before
  public void setUp() throws Exception {
    compiledDAG = CompilerTestUtil.compileALSDAG();
  }

  @Test
  public void testPadoPass() throws Exception {
    final DAG<IRVertex, IREdge> processedDAG = new PadoCompositePass().apply(compiledDAG);

    final IRVertex vertex1 = processedDAG.getTopologicalSort().get(0);
    assertEquals(ExecutorPlacementProperty.TRANSIENT, vertex1.getProperty(ExecutionProperty.Key.ExecutorPlacement));

    final IRVertex vertex5 = processedDAG.getTopologicalSort().get(1);
    assertEquals(ExecutorPlacementProperty.TRANSIENT, vertex5.getProperty(ExecutionProperty.Key.ExecutorPlacement));
    processedDAG.getIncomingEdgesOf(vertex5).forEach(irEdge -> {
      assertEquals(DataStoreProperty.Value.MemoryStore, irEdge.getProperty(ExecutionProperty.Key.DataStore));
      assertEquals(DataFlowModelProperty.Value.Pull, irEdge.getProperty(ExecutionProperty.Key.DataFlowModel));
    });

    final IRVertex vertex6 = processedDAG.getTopologicalSort().get(2);
    assertEquals(ExecutorPlacementProperty.RESERVED, vertex6.getProperty(ExecutionProperty.Key.ExecutorPlacement));
    processedDAG.getIncomingEdgesOf(vertex6).forEach(irEdge -> {
      assertEquals(DataStoreProperty.Value.LocalFileStore, irEdge.getProperty(ExecutionProperty.Key.DataStore));
      assertEquals(DataFlowModelProperty.Value.Push, irEdge.getProperty(ExecutionProperty.Key.DataFlowModel));
    });

    final IRVertex vertex4 = processedDAG.getTopologicalSort().get(6);
    assertEquals(ExecutorPlacementProperty.RESERVED, vertex4.getProperty(ExecutionProperty.Key.ExecutorPlacement));
    processedDAG.getIncomingEdgesOf(vertex4).forEach(irEdge -> {
      assertEquals(DataStoreProperty.Value.MemoryStore, irEdge.getProperty(ExecutionProperty.Key.DataStore));
      assertEquals(DataFlowModelProperty.Value.Pull, irEdge.getProperty(ExecutionProperty.Key.DataFlowModel));
    });

    final IRVertex vertex12 = processedDAG.getTopologicalSort().get(10);
    assertEquals(ExecutorPlacementProperty.TRANSIENT, vertex12.getProperty(ExecutionProperty.Key.ExecutorPlacement));
    processedDAG.getIncomingEdgesOf(vertex12).forEach(irEdge -> {
      assertEquals(DataStoreProperty.Value.LocalFileStore, irEdge.getProperty(ExecutionProperty.Key.DataStore));
      assertEquals(DataFlowModelProperty.Value.Pull, irEdge.getProperty(ExecutionProperty.Key.DataFlowModel));
    });

    final IRVertex vertex14 = processedDAG.getTopologicalSort().get(12);
    assertEquals(ExecutorPlacementProperty.RESERVED, vertex14.getProperty(ExecutionProperty.Key.ExecutorPlacement));
    processedDAG.getIncomingEdgesOf(vertex14).forEach(irEdge -> {
      assertEquals(DataStoreProperty.Value.LocalFileStore, irEdge.getProperty(ExecutionProperty.Key.DataStore));
      assertEquals(DataFlowModelProperty.Value.Push, irEdge.getProperty(ExecutionProperty.Key.DataFlowModel));
    });
  }
}
