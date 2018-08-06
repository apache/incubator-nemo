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
package edu.snu.nemo.compiler.optimizer.pass.compiletime.composite;

import edu.snu.nemo.client.JobLauncher;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.DataFlowProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;
import edu.snu.nemo.compiler.CompilerTestUtil;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.TransientResourceDataStorePass;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.TransientResourcePriorityPass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link TransientResourcePriorityPass} and {@link TransientResourceDataStorePass}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public class TransientResourceCompositePassTest {
  private DAG<IRVertex, IREdge> compiledDAG;

  @Before
  public void setUp() throws Exception {
    compiledDAG = CompilerTestUtil.compileALSDAG();
  }

  @Test
  public void testTransientResourcePass() throws Exception {
    final DAG<IRVertex, IREdge> processedDAG = new TransientResourceCompositePass().apply(compiledDAG);

    final IRVertex vertex1 = processedDAG.getTopologicalSort().get(0);
    assertEquals(ResourcePriorityProperty.TRANSIENT, vertex1.getPropertyValue(ResourcePriorityProperty.class).get());

    final IRVertex vertex5 = processedDAG.getTopologicalSort().get(1);
    assertEquals(ResourcePriorityProperty.TRANSIENT, vertex5.getPropertyValue(ResourcePriorityProperty.class).get());
    processedDAG.getIncomingEdgesOf(vertex5).forEach(irEdge -> {
      assertEquals(DataStoreProperty.Value.MemoryStore, irEdge.getPropertyValue(DataStoreProperty.class).get());
      assertEquals(DataFlowProperty.Value.Pull, irEdge.getPropertyValue(DataFlowProperty.class).get());
    });

    final IRVertex vertex6 = processedDAG.getTopologicalSort().get(2);
    assertEquals(ResourcePriorityProperty.RESERVED, vertex6.getPropertyValue(ResourcePriorityProperty.class).get());
    processedDAG.getIncomingEdgesOf(vertex6).forEach(irEdge -> {
      assertEquals(DataStoreProperty.Value.LocalFileStore, irEdge.getPropertyValue(DataStoreProperty.class).get());
      assertEquals(DataFlowProperty.Value.Push, irEdge.getPropertyValue(DataFlowProperty.class).get());
    });

    final IRVertex vertex4 = processedDAG.getTopologicalSort().get(6);
    assertEquals(ResourcePriorityProperty.RESERVED, vertex4.getPropertyValue(ResourcePriorityProperty.class).get());
    processedDAG.getIncomingEdgesOf(vertex4).forEach(irEdge -> {
      assertEquals(DataStoreProperty.Value.MemoryStore, irEdge.getPropertyValue(DataStoreProperty.class).get());
      assertEquals(DataFlowProperty.Value.Pull, irEdge.getPropertyValue(DataFlowProperty.class).get());
    });

    final IRVertex vertex12 = processedDAG.getTopologicalSort().get(10);
    assertEquals(ResourcePriorityProperty.TRANSIENT, vertex12.getPropertyValue(ResourcePriorityProperty.class).get());
    processedDAG.getIncomingEdgesOf(vertex12).forEach(irEdge -> {
      assertEquals(DataStoreProperty.Value.LocalFileStore, irEdge.getPropertyValue(DataStoreProperty.class).get());
      assertEquals(DataFlowProperty.Value.Pull, irEdge.getPropertyValue(DataFlowProperty.class).get());
    });

    final IRVertex vertex14 = processedDAG.getTopologicalSort().get(12);
    assertEquals(ResourcePriorityProperty.RESERVED, vertex14.getPropertyValue(ResourcePriorityProperty.class).get());
    processedDAG.getIncomingEdgesOf(vertex14).forEach(irEdge -> {
      assertEquals(DataStoreProperty.Value.LocalFileStore, irEdge.getPropertyValue(DataStoreProperty.class).get());
      assertEquals(DataFlowProperty.Value.Push, irEdge.getPropertyValue(DataFlowProperty.class).get());
    });
  }
}
