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
package edu.snu.vortex.compiler.optimizer.pass.compiletime.composite;

import edu.snu.vortex.client.JobLauncher;
import edu.snu.vortex.compiler.CompilerTestUtil;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.vortex.compiler.ir.executionproperty.edge.DataFlowModelProperty;
import edu.snu.vortex.compiler.ir.executionproperty.vertex.ExecutorPlacementProperty;
import edu.snu.vortex.compiler.optimizer.pass.compiletime.annotating.PadoEdgeDataStorePass;
import edu.snu.vortex.compiler.optimizer.pass.compiletime.annotating.PadoVertexPass;
import edu.snu.vortex.runtime.executor.data.LocalFileStore;
import edu.snu.vortex.runtime.executor.data.MemoryStore;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link PadoVertexPass} and {@link PadoEdgeDataStorePass}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public class PadoPassTest {
  private DAG<IRVertex, IREdge> compiledDAG;

  @Before
  public void setUp() throws Exception {
    compiledDAG = CompilerTestUtil.compileALSDAG();
  }

  @Test
  public void testPadoPass() throws Exception {
    final DAG<IRVertex, IREdge> processedDAG = new PadoPass().apply(compiledDAG);

    final IRVertex vertex1 = processedDAG.getTopologicalSort().get(0);
    assertEquals(ExecutorPlacementProperty.TRANSIENT, vertex1.getProperty(ExecutionProperty.Key.ExecutorPlacement));

    final IRVertex vertex5 = processedDAG.getTopologicalSort().get(1);
    assertEquals(ExecutorPlacementProperty.TRANSIENT, vertex5.getProperty(ExecutionProperty.Key.ExecutorPlacement));
    processedDAG.getIncomingEdgesOf(vertex5).forEach(irEdge -> {
      assertEquals(MemoryStore.class, irEdge.getProperty(ExecutionProperty.Key.DataStore));
      assertEquals(DataFlowModelProperty.Value.Pull, irEdge.getProperty(ExecutionProperty.Key.DataFlowModel));
    });

    final IRVertex vertex6 = processedDAG.getTopologicalSort().get(2);
    assertEquals(ExecutorPlacementProperty.RESERVED, vertex6.getProperty(ExecutionProperty.Key.ExecutorPlacement));
    processedDAG.getIncomingEdgesOf(vertex6).forEach(irEdge -> {
      assertEquals(LocalFileStore.class, irEdge.getProperty(ExecutionProperty.Key.DataStore));
      assertEquals(DataFlowModelProperty.Value.Push, irEdge.getProperty(ExecutionProperty.Key.DataFlowModel));
    });

    final IRVertex vertex4 = processedDAG.getTopologicalSort().get(6);
    assertEquals(ExecutorPlacementProperty.RESERVED, vertex4.getProperty(ExecutionProperty.Key.ExecutorPlacement));
    processedDAG.getIncomingEdgesOf(vertex4).forEach(irEdge -> {
      assertEquals(MemoryStore.class, irEdge.getProperty(ExecutionProperty.Key.DataStore));
      assertEquals(DataFlowModelProperty.Value.Pull, irEdge.getProperty(ExecutionProperty.Key.DataFlowModel));
    });

    final IRVertex vertex12 = processedDAG.getTopologicalSort().get(10);
    assertEquals(ExecutorPlacementProperty.RESERVED, vertex12.getProperty(ExecutionProperty.Key.ExecutorPlacement));
    processedDAG.getIncomingEdgesOf(vertex12).forEach(irEdge -> {
      assertEquals(LocalFileStore.class, irEdge.getProperty(ExecutionProperty.Key.DataStore));
      assertEquals(DataFlowModelProperty.Value.Pull, irEdge.getProperty(ExecutionProperty.Key.DataFlowModel));
    });

    final IRVertex vertex13 = processedDAG.getTopologicalSort().get(11);
    assertEquals(ExecutorPlacementProperty.RESERVED, vertex13.getProperty(ExecutionProperty.Key.ExecutorPlacement));
    processedDAG.getIncomingEdgesOf(vertex13).forEach(irEdge -> {
      assertEquals(MemoryStore.class, irEdge.getProperty(ExecutionProperty.Key.DataStore));
      assertEquals(DataFlowModelProperty.Value.Pull, irEdge.getProperty(ExecutionProperty.Key.DataFlowModel));
    });
  }
}
