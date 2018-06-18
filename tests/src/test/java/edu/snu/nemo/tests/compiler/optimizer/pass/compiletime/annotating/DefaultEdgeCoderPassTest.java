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
package edu.snu.nemo.tests.compiler.optimizer.pass.compiletime.annotating;

import edu.snu.nemo.client.JobLauncher;
import edu.snu.nemo.common.coder.Coder;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.CoderProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.AnnotatingPass;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.DefaultEdgeCoderPass;
import edu.snu.nemo.tests.compiler.CompilerTestUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.DefaultEdgeCoderPass}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public class DefaultEdgeCoderPassTest {
  private DAG<IRVertex, IREdge> compiledDAG;

  @Before
  public void setUp() throws Exception {
    compiledDAG = CompilerTestUtil.compileMRDAG();
  }

  @Test
  public void testAnnotatingPass() {
    final AnnotatingPass coderPass = new DefaultEdgeCoderPass();
    assertEquals(CoderProperty.class, coderPass.getExecutionPropertyToModify());
  }

  @Test
  public void testNotOverride() {
    // Get the first coder from the compiled DAG
    final Coder compiledCoder = compiledDAG
        .getOutgoingEdgesOf(compiledDAG.getTopologicalSort().get(0)).get(0).getPropertyValue(CoderProperty.class).get();
    final DAG<IRVertex, IREdge> processedDAG = new DefaultEdgeCoderPass().apply(compiledDAG);

    // Get the first coder from the processed DAG
    final Coder processedCoder = processedDAG.getOutgoingEdgesOf(processedDAG.getTopologicalSort().get(0))
        .get(0).getPropertyValue(CoderProperty.class).get();
    assertEquals(compiledCoder, processedCoder); // It must not be changed.
  }

  @Test
  public void testSetToDefault() throws Exception {
    // Remove the first coder from the compiled DAG (to let our pass to set as default coder).
    compiledDAG.getOutgoingEdgesOf(compiledDAG.getTopologicalSort().get(0))
        .get(0).getExecutionProperties().remove(CoderProperty.class);
    final DAG<IRVertex, IREdge> processedDAG = new DefaultEdgeCoderPass().apply(compiledDAG);

    // Check whether the pass set the empty coder to our default coder.
    final Coder processedCoder = processedDAG.getOutgoingEdgesOf(processedDAG.getTopologicalSort().get(0))
        .get(0).getPropertyValue(CoderProperty.class).get();
    assertEquals(Coder.DUMMY_CODER, processedCoder);
  }
}
