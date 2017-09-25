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
package edu.snu.vortex.compiler.optimizer.pass;

import edu.snu.vortex.client.JobLauncher;
import edu.snu.vortex.compiler.CompilerTestUtil;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.vortex.compiler.ir.executionproperty.edge.DataFlowModelProperty;
import edu.snu.vortex.compiler.ir.executionproperty.vertex.ExecutorPlacementProperty;
import edu.snu.vortex.runtime.executor.data.GlusterFileStore;
import edu.snu.vortex.runtime.executor.data.MemoryStore;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link DisaggregationPass}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public class DisaggregationPassTest {
  private DAG<IRVertex, IREdge> compiledDAG;

  @Before
  public void setUp() throws Exception {
    compiledDAG = CompilerTestUtil.compileALSDAG();
  }

  @Test
  public void testDisaggregation() throws Exception {
    final DAG<IRVertex, IREdge> processedDAG = new DisaggregationPass().apply(compiledDAG);

    processedDAG.getTopologicalSort().forEach(irVertex -> {
      assertEquals(ExecutorPlacementProperty.COMPUTE, irVertex.get(ExecutionProperty.Key.ExecutorPlacement));
      processedDAG.getIncomingEdgesOf(irVertex).forEach(irEdge ->
          assertEquals(DataFlowModelProperty.Value.Pull, irEdge.get(ExecutionProperty.Key.DataFlowModel)));
    });

    final IRVertex vertex4 = processedDAG.getTopologicalSort().get(6);
    processedDAG.getIncomingEdgesOf(vertex4).forEach(irEdge ->
      assertEquals(MemoryStore.class, irEdge.get(ExecutionProperty.Key.DataStore)));
    processedDAG.getOutgoingEdgesOf(vertex4).forEach(irEdge ->
      assertEquals(MemoryStore.class, irEdge.get(ExecutionProperty.Key.DataStore)));

    final IRVertex vertex12 = processedDAG.getTopologicalSort().get(10);
    processedDAG.getIncomingEdgesOf(vertex12).forEach(irEdge ->
      assertEquals(GlusterFileStore.class, irEdge.get(ExecutionProperty.Key.DataStore)));
  }
}
