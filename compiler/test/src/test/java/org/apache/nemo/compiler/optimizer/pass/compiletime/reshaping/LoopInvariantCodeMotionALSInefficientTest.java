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
 * Test {@link LoopOptimizations.LoopInvariantCodeMotionPass} with ALS Inefficient workload.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public class LoopInvariantCodeMotionALSInefficientTest {
  private DAG<IRVertex, IREdge> inefficientALSDAG;
  private DAG<IRVertex, IREdge> groupedDAG;

  @Before
  public void setUp() throws Exception {
    inefficientALSDAG = CompilerTestUtil.compileALSInefficientDAG();
    groupedDAG = new LoopExtractionPass().apply(inefficientALSDAG);
  }

  @Test
  public void testForInefficientALSDAG() throws Exception {
    final long expectedNumOfVertices = groupedDAG.getVertices().size() + 3;

    final DAG<IRVertex, IREdge> processedDAG = LoopOptimizations.getLoopInvariantCodeMotionPass()
        .apply(groupedDAG);
    assertEquals(expectedNumOfVertices, processedDAG.getVertices().size());
  }

}
