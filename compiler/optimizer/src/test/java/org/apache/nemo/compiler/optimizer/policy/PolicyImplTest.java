/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.compiler.optimizer.policy;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.exception.CompileTimeOptimizationException;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.test.EmptyComponents;
import org.apache.nemo.compiler.optimizer.pass.compiletime.CompileTimePass;
import org.apache.nemo.compiler.optimizer.pass.runtime.RunTimePass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertTrue;

public final class PolicyImplTest {
  private IRDAG dag;
  private IRDAG dagForSkew;

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
    final IRDAG optimizedDAG =
      TransientResourcePolicy.BUILDER.build().runCompileTimeOptimization(dag, DAG.EMPTY_DAG_DIRECTORY);
    assertTrue(optimizedDAG.checkIntegrity().isPassed());
  }

  @Test
  public void testDisaggregationPolicy() throws Exception {
    // this should run without an exception.
    final IRDAG optimizedDAG =
      DisaggregationPolicy.BUILDER.build().runCompileTimeOptimization(dag, DAG.EMPTY_DAG_DIRECTORY);
    assertTrue(optimizedDAG.checkIntegrity().isPassed());
  }

  @Test
  public void testDataSkewPolicy() throws Exception {
    // this should run without an exception.
    final IRDAG optimizedDAG =
      DataSkewPolicy.BUILDER.build().runCompileTimeOptimization(dagForSkew, DAG.EMPTY_DAG_DIRECTORY);
    assertTrue(optimizedDAG.checkIntegrity().isPassed());
  }

  @Test
  public void testLargeShufflePolicy() throws Exception {
    // this should run without an exception.
    final IRDAG optimizedDAG =
      LargeShufflePolicy.BUILDER.build().runCompileTimeOptimization(dag, DAG.EMPTY_DAG_DIRECTORY);
    assertTrue(optimizedDAG.checkIntegrity().isPassed());
  }

  @Test
  public void testTransientAndLargeShuffleCombination() throws Exception {
    final List<CompileTimePass> compileTimePasses = new ArrayList<>();
    final Set<RunTimePass<?>> runTimePasses = new HashSet<>();
    compileTimePasses.addAll(LargeShufflePolicy.BUILDER.getCompileTimePasses());
    runTimePasses.addAll(LargeShufflePolicy.BUILDER.getRunTimePasses());
    compileTimePasses.addAll(TransientResourcePolicy.BUILDER.getCompileTimePasses());
    runTimePasses.addAll(TransientResourcePolicy.BUILDER.getRunTimePasses());

    final Policy combinedPolicy = new PolicyImpl(compileTimePasses, runTimePasses);

    // This should NOT throw an exception and work well together.
    final IRDAG optimizedDAG =
      combinedPolicy.runCompileTimeOptimization(dag, DAG.EMPTY_DAG_DIRECTORY);
    assertTrue(optimizedDAG.checkIntegrity().isPassed());
  }

  @Test
  public void testTransientAndDisaggregationCombination() throws Exception {
    final List<CompileTimePass> compileTimePasses = new ArrayList<>();
    final Set<RunTimePass<?>> runTimePasses = new HashSet<>();
    compileTimePasses.addAll(TransientResourcePolicy.BUILDER.getCompileTimePasses());
    runTimePasses.addAll(TransientResourcePolicy.BUILDER.getRunTimePasses());
    compileTimePasses.addAll(DisaggregationPolicy.BUILDER.getCompileTimePasses());
    runTimePasses.addAll(DisaggregationPolicy.BUILDER.getRunTimePasses());

    final Policy combinedPolicy = new PolicyImpl(compileTimePasses, runTimePasses);

    // This should throw an exception.
    // Not all data store should be transferred from and to the GFS.
    expectedException.expect(CompileTimeOptimizationException.class);
    combinedPolicy.runCompileTimeOptimization(dag, DAG.EMPTY_DAG_DIRECTORY);
  }

  @Test
  public void testDataSkewAndLargeShuffleCombination() throws Exception {
    final List<CompileTimePass> compileTimePasses = new ArrayList<>();
    final Set<RunTimePass<?>> runTimePasses = new HashSet<>();
    compileTimePasses.addAll(DataSkewPolicy.BUILDER.getCompileTimePasses());
    runTimePasses.addAll(DataSkewPolicy.BUILDER.getRunTimePasses());
    compileTimePasses.addAll(LargeShufflePolicy.BUILDER.getCompileTimePasses());
    runTimePasses.addAll(LargeShufflePolicy.BUILDER.getRunTimePasses());

    final Policy combinedPolicy = new PolicyImpl(compileTimePasses, runTimePasses);

    // This should throw an exception.
    // DataSizeMetricCollection is not compatible with Push (All data have to be stored before the data collection).
    expectedException.expect(CompileTimeOptimizationException.class);
    combinedPolicy.runCompileTimeOptimization(dagForSkew, DAG.EMPTY_DAG_DIRECTORY);
  }
}
