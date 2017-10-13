/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.onyx.compiler.optimizer.pass.compiletime.annotating;

import edu.snu.onyx.client.JobLauncher;
import edu.snu.onyx.compiler.CompilerTestUtil;
import edu.snu.onyx.compiler.ir.IREdge;
import edu.snu.onyx.compiler.ir.IRVertex;
import edu.snu.onyx.common.dag.DAG;
import edu.snu.onyx.compiler.ir.executionproperty.ExecutionProperty;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link ParallelismPass}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public class ParallelismPassTest {
  @Before
  public void setUp() throws Exception {
  }

  @Test
  public void testAnnotatingPass() {
    final AnnotatingPass parallelismPass = new ParallelismPass();
    assertEquals(ExecutionProperty.Key.Parallelism, parallelismPass.getExecutionPropertyToModify());
  }

  @Test
  public void testParallelism() throws Exception {
    final DAG<IRVertex, IREdge> compiledDAG = CompilerTestUtil.compileALSDAG();
    final DAG<IRVertex, IREdge> processedDAG = new ParallelismPass().apply(compiledDAG);

    processedDAG.getTopologicalSort().forEach(irVertex ->
        assertEquals(1, irVertex.<Integer>getProperty(ExecutionProperty.Key.Parallelism).longValue()));
  }
}
