package org.apache.nemo.compiler.optimizer.pass.compiletime.composite;

import org.apache.nemo.client.JobLauncher;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.compiler.CompilerTestUtil;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.DefaultParallelismPass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.DisaggregationEdgeDataStorePass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.DefaultDataStorePass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link DisaggregationEdgeDataStorePass}.
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
    final DAG<IRVertex, IREdge> processedDAG =
        new DisaggregationEdgeDataStorePass().apply(
            new DefaultDataStorePass().apply(
                  new DefaultParallelismPass().apply(compiledDAG)));

    processedDAG.getTopologicalSort().forEach(irVertex ->
      processedDAG.getIncomingEdgesOf(irVertex).forEach(edgeToMerger ->
          assertEquals(DataStoreProperty.Value.GlusterFileStore,
              edgeToMerger.getPropertyValue(DataStoreProperty.class).get())));
  }
}
