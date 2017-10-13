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
package edu.snu.onyx.compiler.optimizer.pass.compiletime.composite;

import edu.snu.onyx.client.JobLauncher;
import edu.snu.onyx.common.dag.DAG;
import edu.snu.onyx.compiler.CompilerTestUtil;
import edu.snu.onyx.compiler.frontend.beam.transform.GroupByKeyTransform;
import edu.snu.onyx.compiler.ir.MetricCollectionBarrierVertex;
import edu.snu.onyx.compiler.ir.IREdge;
import edu.snu.onyx.compiler.ir.IRVertex;
import edu.snu.onyx.compiler.ir.OperatorVertex;
import edu.snu.onyx.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.onyx.compiler.optimizer.pass.runtime.DataSkewRuntimePass;
import edu.snu.onyx.runtime.executor.datatransfer.communication.ScatterGather;
import edu.snu.onyx.runtime.executor.datatransfer.partitioning.DataSkewHashPartitioner;
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
 * Test {@link DataSkewCompositePass} with MR workload.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public class DataSkewCompositePassTest {
  private DAG<IRVertex, IREdge> mrDAG;
  private static final long NUM_OF_PASSES_IN_DATA_SKEW_PASS = 6;

  @Before
  public void setUp() throws Exception {
  }

  /**
   * Test if the getPassList() method returns the right value upon calling it from a composite pass.
   */
  @Test
  public void testCompositePass() {
    final CompositePass dataSkewPass = new DataSkewCompositePass();
    assertEquals(NUM_OF_PASSES_IN_DATA_SKEW_PASS, dataSkewPass.getPassList().size());

    final Set<ExecutionProperty.Key> prerequisites = new HashSet<>();
    dataSkewPass.getPassList().forEach(compileTimePass ->
        prerequisites.addAll(compileTimePass.getPrerequisiteExecutionProperties()));
    assertEquals(prerequisites, dataSkewPass.getPrerequisiteExecutionProperties());
  }

  /**
   * Test for {@link DataSkewCompositePass} with MR workload. It must insert a {@link MetricCollectionBarrierVertex} before each
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
    final DAG<IRVertex, IREdge> processedDAG = new DataSkewCompositePass().apply(mrDAG);

    assertEquals(originalVerticesNum + numOfScatterGatherEdges, processedDAG.getVertices().size());
    processedDAG.getVertices().stream().filter(irVertex -> irVertex instanceof OperatorVertex
        && ((OperatorVertex) irVertex).getTransform() instanceof GroupByKeyTransform).forEach(irVertex ->
          processedDAG.getIncomingEdgesOf(irVertex).stream().map(IREdge::getSrc).forEach(irVertex1 ->
            assertTrue(irVertex1 instanceof MetricCollectionBarrierVertex)));

    processedDAG.getVertices().forEach(v -> processedDAG.getOutgoingEdgesOf(v).stream()
        .filter(e -> DataSkewRuntimePass.class.equals(e.getProperty(ExecutionProperty.Key.MetricCollection)))
        .forEach(e -> assertEquals(e.getProperty(ExecutionProperty.Key.Partitioner), DataSkewHashPartitioner.class)));
  }
}
