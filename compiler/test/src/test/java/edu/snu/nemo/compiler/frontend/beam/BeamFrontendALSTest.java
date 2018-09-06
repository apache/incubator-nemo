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
package org.apache.nemo.compiler.frontend.beam;

import org.apache.nemo.client.JobLauncher;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.compiler.CompilerTestUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;

/**
 * Test Beam frontend.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public final class BeamFrontendALSTest {
  @Test
  public void testALSDAG() throws Exception {
    final DAG<IRVertex, IREdge> producedDAG = CompilerTestUtil.compileALSDAG();

    assertEquals(producedDAG.getTopologicalSort(), producedDAG.getTopologicalSort());
    assertEquals(42, producedDAG.getVertices().size());

//    producedDAG.getTopologicalSort().forEach(v -> System.out.println(v.getId()));
    final IRVertex vertex11 = producedDAG.getTopologicalSort().get(5);
    assertEquals(1, producedDAG.getIncomingEdgesOf(vertex11).size());
    assertEquals(1, producedDAG.getIncomingEdgesOf(vertex11.getId()).size());
    assertEquals(4, producedDAG.getOutgoingEdgesOf(vertex11).size());

    final IRVertex vertex17 = producedDAG.getTopologicalSort().get(10);
    assertEquals(1, producedDAG.getIncomingEdgesOf(vertex17).size());
    assertEquals(1, producedDAG.getIncomingEdgesOf(vertex17.getId()).size());
    assertEquals(1, producedDAG.getOutgoingEdgesOf(vertex17).size());

    final IRVertex vertex18 = producedDAG.getTopologicalSort().get(16);
    assertEquals(2, producedDAG.getIncomingEdgesOf(vertex18).size());
    assertEquals(2, producedDAG.getIncomingEdgesOf(vertex18.getId()).size());
    assertEquals(1, producedDAG.getOutgoingEdgesOf(vertex18).size());
  }
}
