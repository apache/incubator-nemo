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
package edu.snu.nemo.compiler.optimizer.pass.compiletime.composite;

import edu.snu.nemo.client.JobLauncher;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.MetricCollectionProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.PartitionerProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.MetricCollectionBarrierVertex;
import edu.snu.nemo.common.ir.executionproperty.ExecutionProperty;
import edu.snu.nemo.compiler.CompilerTestUtil;
import edu.snu.nemo.common.ir.vertex.executionproperty.SkewnessAwareSchedulingProperty;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.AnnotatingPass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
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
  private static final long NUM_OF_PASSES_IN_DATA_SKEW_PASS = 5;

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

    final Set<Class<? extends ExecutionProperty>> prerequisites = new HashSet<>();
    dataSkewPass.getPassList().forEach(compileTimePass ->
        prerequisites.addAll(compileTimePass.getPrerequisiteExecutionProperties()));
    dataSkewPass.getPassList().forEach(compileTimePass -> {
      if (compileTimePass instanceof AnnotatingPass) {
        prerequisites.remove(((AnnotatingPass) compileTimePass).getExecutionPropertyToModify());
      }
    });
    assertEquals(prerequisites, dataSkewPass.getPrerequisiteExecutionProperties());
  }

  /**
   * Test for {@link DataSkewCompositePass} with MR workload. It must insert a {@link MetricCollectionBarrierVertex}
   * before each shuffle edge.
   * @throws Exception exception on the way.
   */
  @Test
  public void testDataSkewPass() throws Exception {
    mrDAG = CompilerTestUtil.compileWordCountDAG();
    final Integer originalVerticesNum = mrDAG.getVertices().size();
    final Long numOfShuffleGatherEdges = mrDAG.getVertices().stream().filter(irVertex ->
        mrDAG.getIncomingEdgesOf(irVertex).stream().anyMatch(irEdge ->
            DataCommunicationPatternProperty.Value.Shuffle
            .equals(irEdge.getPropertyValue(DataCommunicationPatternProperty.class).get())))
        .count();
    final DAG<IRVertex, IREdge> processedDAG = new DataSkewCompositePass().apply(mrDAG);

    assertEquals(originalVerticesNum + numOfShuffleGatherEdges, processedDAG.getVertices().size());
    processedDAG.getVertices().stream().map(processedDAG::getIncomingEdgesOf)
        .flatMap(List::stream)
        .filter(irEdge -> DataCommunicationPatternProperty.Value.Shuffle
            .equals(irEdge.getPropertyValue(DataCommunicationPatternProperty.class).get()))
        .map(IREdge::getSrc)
        .forEach(irVertex -> assertTrue(irVertex instanceof MetricCollectionBarrierVertex));

    processedDAG.getVertices().forEach(v -> processedDAG.getOutgoingEdgesOf(v).stream()
        .filter(e -> Optional.of(MetricCollectionProperty.Value.DataSkewRuntimePass)
                  .equals(e.getPropertyValue(MetricCollectionProperty.class)))
        .forEach(e -> assertEquals(PartitionerProperty.Value.DataSkewHashPartitioner,
            e.getPropertyValue(PartitionerProperty.class).get())));

    processedDAG.filterVertices(v -> v instanceof MetricCollectionBarrierVertex)
        .forEach(metricV -> {
          List<IRVertex> reducerV = processedDAG.getChildren(metricV.getId());
          reducerV.forEach(rV -> assertTrue(rV.getPropertyValue(SkewnessAwareSchedulingProperty.class).get()));
        });
  }
}
