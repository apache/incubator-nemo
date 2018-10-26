package org.apache.nemo.compiler.frontend.beam;

import org.apache.nemo.client.JobLauncher;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.compiler.CompilerTestUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;

/**
 * Test Beam frontend.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public final class BeamFrontendALSTest {
  @Test
  public void testALSDAG() throws Exception {
    final DAG<IRVertex, IREdge> producedDAG = CompilerTestUtil.compileALSDAG();

    assertEquals(producedDAG.getTopologicalSort(), producedDAG.getTopologicalSort());
    assertEquals(42, producedDAG.getVertices().size());

//    producedDAG.getTopologicalSort().forEach(v -> System.out.println(v.getId()));
    final IRVertex vertex11 = producedDAG.getTopologicalSort().get(5);
    assertEquals(1, producedDAG.getIncomingEdgesOf(vertex11).size());
    assertEquals(1, producedDAG.getIncomingEdgesOf(vertex11.getId()).size());
    assertEquals(4, producedDAG.getOutgoingEdgesOf(vertex11).size());

    final IRVertex vertex17 = producedDAG.getTopologicalSort().get(10);
    assertEquals(1, producedDAG.getIncomingEdgesOf(vertex17).size());
    assertEquals(1, producedDAG.getIncomingEdgesOf(vertex17.getId()).size());
    assertEquals(1, producedDAG.getOutgoingEdgesOf(vertex17).size());

    final IRVertex vertex18 = producedDAG.getTopologicalSort().get(16);
    assertEquals(2, producedDAG.getIncomingEdgesOf(vertex18).size());
    assertEquals(2, producedDAG.getIncomingEdgesOf(vertex18.getId()).size());
    assertEquals(1, producedDAG.getOutgoingEdgesOf(vertex18).size());
  }
}
