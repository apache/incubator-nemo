package org.apache.nemo.compiler.optimizer.policy;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.exception.CompileTimeOptimizationException;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.test.EmptyComponents;
import org.apache.nemo.compiler.optimizer.pass.compiletime.CompileTimePass;
import org.apache.nemo.runtime.common.optimizer.pass.runtime.RuntimePass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;

public final class PolicyImplTest {
  private DAG dag;
  private DAG dagForSkew;

  @Before
  public void setUp() {
    this.dag = EmptyComponents.buildEmptyDAG();
    this.dagForSkew = EmptyComponents.buildEmptyDAGForSkew();
  }

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testTransientPolicy() throws Exception {
    // this should run without an exception.
    TransientResourcePolicy.BUILDER.build().runCompileTimeOptimization(dag, DAG.EMPTY_DAG_DIRECTORY);
  }

  @Test
  public void testDisaggregationPolicy() throws Exception {
    // this should run without an exception.
    DisaggregationPolicy.BUILDER.build().runCompileTimeOptimization(dag, DAG.EMPTY_DAG_DIRECTORY);
  }

  @Test
  public void testDataSkewPolicy() throws Exception {
    // this should run without an exception.
    DataSkewPolicy.BUILDER.build().runCompileTimeOptimization(dagForSkew, DAG.EMPTY_DAG_DIRECTORY);
  }

  @Test
  public void testLargeShufflePolicy() throws Exception {
    // this should run without an exception.
    LargeShufflePolicy.BUILDER.build().runCompileTimeOptimization(dag, DAG.EMPTY_DAG_DIRECTORY);
  }

  @Test
  public void testTransientAndLargeShuffleCombination() throws Exception {
    final List<CompileTimePass> compileTimePasses = new ArrayList<>();
    final List<RuntimePass<?>> runtimePasses = new ArrayList<>();
    compileTimePasses.addAll(TransientResourcePolicy.BUILDER.getCompileTimePasses());
    runtimePasses.addAll(TransientResourcePolicy.BUILDER.getRuntimePasses());
    compileTimePasses.addAll(LargeShufflePolicy.BUILDER.getCompileTimePasses());
    runtimePasses.addAll(LargeShufflePolicy.BUILDER.getRuntimePasses());

    final Policy combinedPolicy = new PolicyImpl(compileTimePasses, runtimePasses);

    // This should NOT throw an exception and work well together.
    combinedPolicy.runCompileTimeOptimization(dag, DAG.EMPTY_DAG_DIRECTORY);
  }

  @Test
  public void testTransientAndDisaggregationCombination() throws Exception {
    final List<CompileTimePass> compileTimePasses = new ArrayList<>();
    final List<RuntimePass<?>> runtimePasses = new ArrayList<>();
    compileTimePasses.addAll(TransientResourcePolicy.BUILDER.getCompileTimePasses());
    runtimePasses.addAll(TransientResourcePolicy.BUILDER.getRuntimePasses());
    compileTimePasses.addAll(DisaggregationPolicy.BUILDER.getCompileTimePasses());
    runtimePasses.addAll(DisaggregationPolicy.BUILDER.getRuntimePasses());

    final Policy combinedPolicy = new PolicyImpl(compileTimePasses, runtimePasses);

    // This should throw an exception.
    // Not all data store should be transferred from and to the GFS.
    expectedException.expect(CompileTimeOptimizationException.class);
    combinedPolicy.runCompileTimeOptimization(dag, DAG.EMPTY_DAG_DIRECTORY);
  }

  @Test
  public void testDataSkewAndLargeShuffleCombination() throws Exception {
    final List<CompileTimePass> compileTimePasses = new ArrayList<>();
    final List<RuntimePass<?>> runtimePasses = new ArrayList<>();
    compileTimePasses.addAll(DataSkewPolicy.BUILDER.getCompileTimePasses());
    runtimePasses.addAll(DataSkewPolicy.BUILDER.getRuntimePasses());
    compileTimePasses.addAll(LargeShufflePolicy.BUILDER.getCompileTimePasses());
    runtimePasses.addAll(LargeShufflePolicy.BUILDER.getRuntimePasses());

    final Policy combinedPolicy = new PolicyImpl(compileTimePasses, runtimePasses);

    // This should throw an exception.
    // DataSizeMetricCollection is not compatible with Push (All data have to be stored before the data collection).
    expectedException.expect(CompileTimeOptimizationException.class);
    combinedPolicy.runCompileTimeOptimization(dagForSkew, DAG.EMPTY_DAG_DIRECTORY);
  }
}
