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
public class BeamFrontendMLRTest {
  @Test
  public void testMLRDAG() throws Exception {
    final DAG<IRVertex, IREdge> producedDAG = CompilerTestUtil.compileMLRDAG();

    assertEquals(producedDAG.getTopologicalSort(), producedDAG.getTopologicalSort());
    assertEquals(36, producedDAG.getVertices().size());

    final IRVertex vertex1 = producedDAG.getTopologicalSort().get(5);
    assertEquals(1, producedDAG.getIncomingEdgesOf(vertex1).size());
    assertEquals(0, producedDAG.getIncomingEdgesOf(vertex1.getId()).size());
    assertEquals(3, producedDAG.getOutgoingEdgesOf(vertex1).size());

    final IRVertex vertex15 = producedDAG.getTopologicalSort().get(13);
    assertEquals(1, producedDAG.getIncomingEdgesOf(vertex15).size());
    assertEquals(1, producedDAG.getIncomingEdgesOf(vertex15.getId()).size());
    assertEquals(1, producedDAG.getOutgoingEdgesOf(vertex15).size());

    final IRVertex vertex21 = producedDAG.getTopologicalSort().get(19);
    assertEquals(2, producedDAG.getIncomingEdgesOf(vertex21).size());
    assertEquals(2, producedDAG.getIncomingEdgesOf(vertex21.getId()).size());
    assertEquals(1, producedDAG.getOutgoingEdgesOf(vertex21).size());
  }
}
