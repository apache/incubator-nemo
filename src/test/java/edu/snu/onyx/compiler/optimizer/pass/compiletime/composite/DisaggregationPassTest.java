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
package edu.snu.onyx.compiler.optimizer.pass.compiletime.composite;

import edu.snu.onyx.client.JobLauncher;
import edu.snu.onyx.compiler.CompilerTestUtil;
import edu.snu.onyx.compiler.ir.IREdge;
import edu.snu.onyx.compiler.ir.IRVertex;
import edu.snu.onyx.common.dag.DAG;
import edu.snu.onyx.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.onyx.compiler.ir.executionproperty.edge.DataFlowModelProperty;
import edu.snu.onyx.compiler.ir.executionproperty.vertex.ExecutorPlacementProperty;
import edu.snu.onyx.runtime.executor.data.GlusterFileStore;
import edu.snu.onyx.runtime.executor.data.MemoryStore;
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
      assertEquals(ExecutorPlacementProperty.COMPUTE, irVertex.getProperty(ExecutionProperty.Key.ExecutorPlacement));
      processedDAG.getIncomingEdgesOf(irVertex).forEach(irEdge ->
          assertEquals(DataFlowModelProperty.Value.Pull, irEdge.getProperty(ExecutionProperty.Key.DataFlowModel)));
    });

    final IRVertex vertex4 = processedDAG.getTopologicalSort().get(6);
    processedDAG.getIncomingEdgesOf(vertex4).forEach(irEdge ->
      assertEquals(MemoryStore.class, irEdge.getProperty(ExecutionProperty.Key.DataStore)));
    processedDAG.getOutgoingEdgesOf(vertex4).forEach(irEdge ->
      assertEquals(MemoryStore.class, irEdge.getProperty(ExecutionProperty.Key.DataStore)));

    final IRVertex vertex12 = processedDAG.getTopologicalSort().get(10);
    processedDAG.getIncomingEdgesOf(vertex12).forEach(irEdge ->
      assertEquals(GlusterFileStore.class, irEdge.getProperty(ExecutionProperty.Key.DataStore)));
  }
}
