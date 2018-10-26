package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.client.JobLauncher;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.compiler.CompilerTestUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test {@link DefaultParallelismPass}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public class DefaultParallelismPassTest {
  private DAG<IRVertex, IREdge> compiledDAG;

  @Before
  public void setUp() throws Exception {
    compiledDAG = CompilerTestUtil.compileALSDAG();
  }

  @Test
  public void testAnnotatingPass() {
    final AnnotatingPass parallelismPass = new DefaultParallelismPass();
    assertTrue(parallelismPass.getExecutionPropertiesToAnnotate().contains(ParallelismProperty.class));
  }

  @Test
  public void testParallelismOne() {
    final DAG<IRVertex, IREdge> processedDAG = new DefaultParallelismPass().apply(compiledDAG);

    processedDAG.getTopologicalSort().forEach(irVertex ->
        assertEquals(1, irVertex.getPropertyValue(ParallelismProperty.class).get().longValue()));
  }

  @Test
  public void testParallelismTen() {
    final int desiredSourceParallelism = 10;
    final DAG<IRVertex, IREdge> processedDAG = new DefaultParallelismPass(desiredSourceParallelism, 2).apply(compiledDAG);

    processedDAG.getTopologicalSort().stream()
        .filter(irVertex -> irVertex instanceof SourceVertex)
        .forEach(irVertex -> assertEquals(desiredSourceParallelism,
            irVertex.getPropertyValue(ParallelismProperty.class).get().longValue()));
  }
}
