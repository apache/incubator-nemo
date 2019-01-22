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
package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.client.JobLauncher;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.IRDAG;
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
  private IRDAG compiledDAG;

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
    final IRDAG processedDAG = new DefaultParallelismPass().optimize(compiledDAG);

    processedDAG.getTopologicalSort().forEach(irVertex ->
        assertEquals(1, irVertex.getPropertyValue(ParallelismProperty.class).get().longValue()));
  }

  @Test
  public void testParallelismTen() {
    final int desiredSourceParallelism = 10;
    final IRDAG processedDAG = new DefaultParallelismPass(desiredSourceParallelism, 2).optimize(compiledDAG);

    processedDAG.getTopologicalSort().stream()
        .filter(irVertex -> irVertex instanceof SourceVertex)
        .forEach(irVertex -> assertEquals(desiredSourceParallelism,
            irVertex.getPropertyValue(ParallelismProperty.class).get().longValue()));
  }
}
