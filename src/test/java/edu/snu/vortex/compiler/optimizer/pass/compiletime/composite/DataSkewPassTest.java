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
package edu.snu.vortex.compiler.optimizer.pass.compiletime.composite;

import edu.snu.vortex.client.JobLauncher;
import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.compiler.CompilerTestUtil;
import edu.snu.vortex.compiler.frontend.beam.transform.GroupByKeyTransform;
import edu.snu.vortex.compiler.ir.MetricCollectionBarrierVertex;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.OperatorVertex;
import edu.snu.vortex.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.vortex.runtime.executor.datatransfer.data_communication_pattern.ScatterGather;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test {@link DataSkewPass} with MR workload.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public class DataSkewPassTest {
  private DAG<IRVertex, IREdge> mrDAG;
  private static final long NUM_OF_PASSES_IN_DATA_SKEW_PASS = 4;

  @Before
  public void setUp() throws Exception {
  }

  /**
   * Test if the getpassList() method returns the right value upon calling it from a composite pass.
   */
  @Test
  public void testCompositePass() {
    final CompositePass dataSkewPass = new DataSkewPass();
    assertEquals(NUM_OF_PASSES_IN_DATA_SKEW_PASS, dataSkewPass.getPassList().size());

    final Set<ExecutionProperty.Key> prerequisites = new HashSet<>();
    dataSkewPass.getPassList().forEach(compileTimePass ->
        prerequisites.addAll(compileTimePass.getPrerequisiteExecutionProperties()));
    assertEquals(prerequisites, dataSkewPass.getPrerequisiteExecutionProperties());
  }

  /**
   * Test for {@link DataSkewPass} with MR workload. It must insert a {@link MetricCollectionBarrierVertex} before each
   * {@link OperatorVertex} with {@link GroupByKeyTransform}.
   * @throws Exception exception on the way.
   */
  @Test
  public void testDataSkewPass() throws Exception {
    mrDAG = CompilerTestUtil.compileMRDAG();
    final Integer originalVerticesNum = mrDAG.getVertices().size();
    final Long numOfScatterGatherEdges = mrDAG.getVertices().stream().filter(irVertex ->
        mrDAG.getIncomingEdgesOf(irVertex).stream().anyMatch(irEdge ->
            ScatterGather.class.equals(irEdge.getProperty(ExecutionProperty.Key.DataCommunicationPattern))))
        .count();
    final DAG<IRVertex, IREdge> processedDAG = new DataSkewPass().apply(mrDAG);

    assertEquals(originalVerticesNum + numOfScatterGatherEdges, processedDAG.getVertices().size());
    processedDAG.getVertices().stream().filter(irVertex -> irVertex instanceof OperatorVertex
        && ((OperatorVertex) irVertex).getTransform() instanceof GroupByKeyTransform).forEach(irVertex ->
          processedDAG.getIncomingEdgesOf(irVertex).stream().map(IREdge::getSrc).forEach(irVertex1 ->
            assertTrue(irVertex1 instanceof MetricCollectionBarrierVertex)));
  }
}
