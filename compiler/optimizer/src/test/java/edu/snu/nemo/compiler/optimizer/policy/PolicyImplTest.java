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

package edu.snu.nemo.compiler.optimizer.policy;

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.exception.CompileTimeOptimizationException;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.OperatorVertex;
import edu.snu.nemo.common.test.EmptyComponents;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.CompileTimePass;
import edu.snu.nemo.runtime.common.optimizer.pass.runtime.RuntimePass;
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
