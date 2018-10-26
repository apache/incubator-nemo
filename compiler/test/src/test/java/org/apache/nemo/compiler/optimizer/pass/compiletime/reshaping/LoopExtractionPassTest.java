package org.apache.nemo.compiler.optimizer.pass.compiletime.reshaping;

import org.apache.nemo.client.JobLauncher;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.compiler.CompilerTestUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link LoopExtractionPass}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public class LoopExtractionPassTest {
  private DAG<IRVertex, IREdge> compiledDAG;

  @Before
  public void setUp() throws Exception {
    compiledDAG = CompilerTestUtil.compileALSDAG();
  }

  @Test
  public void testLoopGrouping() {
    final DAG<IRVertex, IREdge> processedDAG = new LoopExtractionPass().apply(compiledDAG);

    assertEquals(13, processedDAG.getTopologicalSort().size());
  }
}
