package org.apache.nemo.compiler.optimizer.pass.compiletime.reshaping;

import org.apache.nemo.client.JobLauncher;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.compiler.CompilerTestUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link LoopUnrollingPass}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public class LoopUnrollingPassTest {
  private DAG<IRVertex, IREdge> compiledDAG;

  @Before
  public void setUp() throws Exception {
    compiledDAG = CompilerTestUtil.compileALSDAG();
  }

  @Test
  public void testLoopUnrollingPass() throws Exception {
    final DAG<IRVertex, IREdge> processedDAG =
        new LoopUnrollingPass().apply(new LoopExtractionPass().apply(compiledDAG));

    assertEquals(compiledDAG.getTopologicalSort().size(), processedDAG.getTopologicalSort().size());
    // zip vertices
    final Iterator<IRVertex> vertices1 = compiledDAG.getTopologicalSort().iterator();
    final Iterator<IRVertex> vertices2 = processedDAG.getTopologicalSort().iterator();
    final List<Pair<IRVertex, IRVertex>> list = new ArrayList<>();
    while  (vertices1.hasNext() && vertices2.hasNext()) {
      list.add(Pair.of(vertices1.next(), vertices2.next()));
    }
    list.forEach(irVertexPair -> {
        assertEquals(irVertexPair.left().getExecutionProperties(), irVertexPair.right().getExecutionProperties());
        assertEquals(compiledDAG.getIncomingEdgesOf(irVertexPair.left()).size(),
            processedDAG.getIncomingEdgesOf(irVertexPair.right()).size());
    });
  }
}
