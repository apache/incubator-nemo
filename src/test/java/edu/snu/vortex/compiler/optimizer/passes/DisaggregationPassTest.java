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
package edu.snu.vortex.compiler.optimizer.passes;

import edu.snu.vortex.client.JobLauncher;
import edu.snu.vortex.compiler.TestUtil;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.common.dag.DAG;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link DisaggregationPass}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public class DisaggregationPassTest {
  private DAG<IRVertex, IREdge> compiledDAG;

  @Before
  public void setUp() throws Exception {
    compiledDAG = TestUtil.compileALSDAG();
  }

  @Test
  public void testDisaggregation() throws Exception {
    final DAG<IRVertex, IREdge> processedDAG = new DisaggregationPass().process(compiledDAG);

    processedDAG.getTopologicalSort().forEach(irVertex -> {
      assertEquals(Attribute.Compute, irVertex.getAttr(Attribute.Key.Placement));
      processedDAG.getIncomingEdgesOf(irVertex).forEach(irEdge ->
          assertEquals(Attribute.Pull, irEdge.getAttr(Attribute.Key.ChannelTransferPolicy)));
    });

    final IRVertex vertex4 = processedDAG.getTopologicalSort().get(6);
    processedDAG.getIncomingEdgesOf(vertex4).forEach(irEdge ->
      assertEquals(Attribute.Memory, irEdge.getAttr(Attribute.Key.ChannelDataPlacement)));
    processedDAG.getOutgoingEdgesOf(vertex4).forEach(irEdge ->
      assertEquals(Attribute.Memory, irEdge.getAttr(Attribute.Key.ChannelDataPlacement)));

    final IRVertex vertex12 = processedDAG.getTopologicalSort().get(10);
    processedDAG.getIncomingEdgesOf(vertex12).forEach(irEdge ->
      assertEquals(Attribute.RemoteFile, irEdge.getAttr(Attribute.Key.ChannelDataPlacement)));
  }
}
