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
package edu.snu.nemo.compiler.optimizer.pass.compiletime.reshaping;

import edu.snu.nemo.client.JobLauncher;
import edu.snu.nemo.common.Pair;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.compiler.CompilerTestUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link LoopUnrollingPass}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public class LoopUnrollingPassTest {
  private DAG<IRVertex, IREdge> compiledDAG;

  @Before
  public void setUp() throws Exception {
    compiledDAG = CompilerTestUtil.compileALSDAG();
  }

  @Test
  public void testLoopUnrollingPass() throws Exception {
    final DAG<IRVertex, IREdge> processedDAG =
        new LoopUnrollingPass().apply(new LoopExtractionPass().apply(compiledDAG));

    assertEquals(compiledDAG.getTopologicalSort().size(), processedDAG.getTopologicalSort().size());
    // zip vertices
    final Iterator<IRVertex> vertices1 = compiledDAG.getTopologicalSort().iterator();
    final Iterator<IRVertex> vertices2 = processedDAG.getTopologicalSort().iterator();
    final List<Pair<IRVertex, IRVertex>> list = new ArrayList<>();
    while  (vertices1.hasNext() && vertices2.hasNext()) {
      list.add(Pair.of(vertices1.next(), vertices2.next()));
    }
    list.forEach(irVertexPair -> {
        assertEquals(irVertexPair.left().getExecutionProperties(), irVertexPair.right().getExecutionProperties());
        assertEquals(compiledDAG.getIncomingEdgesOf(irVertexPair.left()).size(),
            processedDAG.getIncomingEdgesOf(irVertexPair.right()).size());
    });
  }
}
