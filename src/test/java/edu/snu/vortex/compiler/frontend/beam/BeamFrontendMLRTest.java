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
package edu.snu.vortex.compiler.frontend.beam;

import edu.snu.vortex.client.JobLauncher;
import edu.snu.vortex.compiler.TestUtil;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.common.dag.DAG;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link BeamFrontend}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public class BeamFrontendMLRTest {
  @Test
  public void testMLRDAG() throws Exception {
    final DAG<IRVertex, IREdge> producedDAG = TestUtil.compileMLRDAG();

    assertEquals(producedDAG.getTopologicalSort(), producedDAG.getTopologicalSort());
    assertEquals(33, producedDAG.getVertices().size());

//    producedDAG.getTopologicalSort().forEach(v -> System.out.println(v.getId()));
    final IRVertex vertex3 = producedDAG.getTopologicalSort().get(0);
    assertEquals(0, producedDAG.getIncomingEdgesOf(vertex3).size());
    assertEquals(0, producedDAG.getIncomingEdgesOf(vertex3.getId()).size());
    assertEquals(3, producedDAG.getOutgoingEdgesOf(vertex3).size());

    final IRVertex vertex12 = producedDAG.getTopologicalSort().get(10);
    assertEquals(1, producedDAG.getIncomingEdgesOf(vertex12).size());
    assertEquals(1, producedDAG.getIncomingEdgesOf(vertex12.getId()).size());
    assertEquals(1, producedDAG.getOutgoingEdgesOf(vertex12).size());

    final IRVertex vertex17 = producedDAG.getTopologicalSort().get(15);
    assertEquals(2, producedDAG.getIncomingEdgesOf(vertex17).size());
    assertEquals(2, producedDAG.getIncomingEdgesOf(vertex17.getId()).size());
    assertEquals(1, producedDAG.getOutgoingEdgesOf(vertex17).size());
  }
}
