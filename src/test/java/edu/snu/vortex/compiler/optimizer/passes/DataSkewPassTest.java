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
import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.compiler.TestUtil;
import edu.snu.vortex.compiler.frontend.beam.transform.GroupByKeyTransform;
import edu.snu.vortex.compiler.ir.MetricCollectionBarrierVertex;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.OperatorVertex;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test {@link DataSkewPass} with MR workload.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public class DataSkewPassTest {
  private DAG<IRVertex, IREdge> mrDAG;

  @Before
  public void setUp() throws Exception {
    mrDAG = TestUtil.compileMRDAG();
  }

  /**
   * Test for {@link DataSkewPass} with MR workload. It must insert a {@link MetricCollectionBarrierVertex} before each
   * {@link OperatorVertex} with {@link GroupByKeyTransform}.
   * @throws Exception exception on the way.
   */
  @Test
  public void testDataSkewPass() throws Exception {
    final Integer originalVerticesNum = mrDAG.getVertices().size();
    final Long numOfGroupByKeyOperatorVertices = mrDAG.getVertices().stream().filter(irVertex ->
        irVertex instanceof OperatorVertex
            && ((OperatorVertex) irVertex).getTransform() instanceof GroupByKeyTransform).count();
    final DAG<IRVertex, IREdge> processedDAG = new DataSkewPass().process(mrDAG);

    assertEquals(originalVerticesNum + numOfGroupByKeyOperatorVertices, processedDAG.getVertices().size());
    processedDAG.getVertices().stream().filter(irVertex -> irVertex instanceof OperatorVertex
        && ((OperatorVertex) irVertex).getTransform() instanceof GroupByKeyTransform).forEach(irVertex ->
          processedDAG.getIncomingEdgesOf(irVertex).stream().map(IREdge::getSrc).forEach(irVertex1 ->
            assertTrue(irVertex1 instanceof MetricCollectionBarrierVertex)));
  }
}
