package org.apache.nemo.compiler.optimizer.pass.compiletime.composite;

import org.apache.nemo.client.JobLauncher;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.DataFlowProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;
import org.apache.nemo.compiler.CompilerTestUtil;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.TransientResourceDataStorePass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.TransientResourcePriorityPass;
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

    final IRVertex vertex2 = processedDAG.getTopologicalSort().get(11);
    assertEquals(ResourcePriorityProperty.TRANSIENT, vertex2.getPropertyValue(ResourcePriorityProperty.class).get());
    processedDAG.getIncomingEdgesOf(vertex2).forEach(irEdge -> {
      assertEquals(DataStoreProperty.Value.MemoryStore, irEdge.getPropertyValue(DataStoreProperty.class).get());
      assertEquals(DataFlowProperty.Value.Pull, irEdge.getPropertyValue(DataFlowProperty.class).get());
    });

    final IRVertex vertex5 = processedDAG.getTopologicalSort().get(14);
    assertEquals(ResourcePriorityProperty.RESERVED, vertex5.getPropertyValue(ResourcePriorityProperty.class).get());
    processedDAG.getIncomingEdgesOf(vertex5).forEach(irEdge -> {
      assertEquals(DataStoreProperty.Value.LocalFileStore, irEdge.getPropertyValue(DataStoreProperty.class).get());
      assertEquals(DataFlowProperty.Value.Push, irEdge.getPropertyValue(DataFlowProperty.class).get());
    });

    final IRVertex vertex11 = processedDAG.getTopologicalSort().get(5);
    assertEquals(ResourcePriorityProperty.RESERVED, vertex11.getPropertyValue(ResourcePriorityProperty.class).get());
    processedDAG.getIncomingEdgesOf(vertex11).forEach(irEdge -> {
      assertEquals(DataStoreProperty.Value.MemoryStore, irEdge.getPropertyValue(DataStoreProperty.class).get());
      assertEquals(DataFlowProperty.Value.Pull, irEdge.getPropertyValue(DataFlowProperty.class).get());
    });

    final IRVertex vertex17 = processedDAG.getTopologicalSort().get(10);
    assertEquals(ResourcePriorityProperty.TRANSIENT, vertex17.getPropertyValue(ResourcePriorityProperty.class).get());
    processedDAG.getIncomingEdgesOf(vertex17).forEach(irEdge -> {
      assertEquals(DataStoreProperty.Value.LocalFileStore, irEdge.getPropertyValue(DataStoreProperty.class).get());
      assertEquals(DataFlowProperty.Value.Pull, irEdge.getPropertyValue(DataFlowProperty.class).get());
    });

    final IRVertex vertex19 = processedDAG.getTopologicalSort().get(17);
    assertEquals(ResourcePriorityProperty.RESERVED, vertex19.getPropertyValue(ResourcePriorityProperty.class).get());
    processedDAG.getIncomingEdgesOf(vertex19).forEach(irEdge -> {
      assertEquals(DataStoreProperty.Value.LocalFileStore, irEdge.getPropertyValue(DataStoreProperty.class).get());
      assertEquals(DataFlowProperty.Value.Push, irEdge.getPropertyValue(DataFlowProperty.class).get());
    });
  }
}
