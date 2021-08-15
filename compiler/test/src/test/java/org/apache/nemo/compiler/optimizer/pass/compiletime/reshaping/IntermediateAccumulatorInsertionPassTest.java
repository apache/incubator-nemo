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
package org.apache.nemo.compiler.optimizer.pass.compiletime.reshaping;

import org.apache.nemo.client.JobLauncher;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.vertex.executionproperty.ShuffleExecutorSetProperty;
import org.apache.nemo.compiler.CompilerTestUtil;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.*;
import org.apache.nemo.compiler.optimizer.policy.Policy;
import org.apache.nemo.compiler.optimizer.policy.PolicyBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static junit.framework.TestCase.assertTrue;
import static org.apache.nemo.common.dag.DAG.EMPTY_DAG_DIRECTORY;

/**
 * Test {@link IntermediateAccumulatorInsertionPass}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public class IntermediateAccumulatorInsertionPassTest {
  private IRDAG compiledDAG;

  @Before
  public void setUp() throws Exception {
    compiledDAG = CompilerTestUtil.compileEDGARDAG();
  }

  @Test
  public void testIntermediateAccumulatorInsertionPass() {
    final PolicyBuilder builder = new PolicyBuilder();
    builder.registerCompileTimePass(new DefaultParallelismPass(16, 2))
      .registerCompileTimePass(new DefaultEdgeEncoderPass())
      .registerCompileTimePass(new DefaultEdgeDecoderPass())
      .registerCompileTimePass(new DefaultDataStorePass())
      .registerCompileTimePass(new DefaultDataPersistencePass())
      .registerCompileTimePass(new DefaultScheduleGroupPass())
      .registerCompileTimePass(new CompressionPass())
      .registerCompileTimePass(new ResourceLocalityPass())
      .registerCompileTimePass(new ResourceSlotPass())
      .registerCompileTimePass(new PipeTransferForAllEdgesPass())
      .registerCompileTimePass(new IntermediateAccumulatorInsertionPass(true));
    final Policy policy = builder.build();
    compiledDAG = policy.runCompileTimeOptimization(compiledDAG, EMPTY_DAG_DIRECTORY);
    assertTrue(compiledDAG.getTopologicalSort().stream()
      .anyMatch(v -> v.getPropertyValue(ShuffleExecutorSetProperty.class).isPresent()));
    assertTrue(compiledDAG.checkIntegrity().isPassed());
  }
}
