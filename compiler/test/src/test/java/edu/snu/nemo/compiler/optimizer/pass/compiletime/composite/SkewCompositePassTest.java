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
import edu.snu.nemo.common.ir.edge.executionproperty.AdditionalOutputTagProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.executionproperty.ExecutionProperty;
import edu.snu.nemo.common.ir.vertex.OperatorVertex;
import edu.snu.nemo.common.ir.vertex.transform.MetricCollectTransform;
import edu.snu.nemo.common.ir.vertex.transform.AggregateMetricTransform;
import edu.snu.nemo.compiler.CompilerTestUtil;
import edu.snu.nemo.common.ir.vertex.executionproperty.ResourceSkewedDataProperty;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.AnnotatingPass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test {@link SkewCompositePass} with MR workload.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public class SkewCompositePassTest {
  private DAG<IRVertex, IREdge> mrDAG;
  private static final long NUM_OF_PASSES_IN_DATA_SKEW_PASS = 3;

  @Before
  public void setUp() throws Exception {
  }

  /**
   * Test if the getPassList() method returns the right value upon calling it from a composite pass.
   */
  @Test
  public void testCompositePass() {
    final CompositePass dataSkewPass = new SkewCompositePass();
    assertEquals(NUM_OF_PASSES_IN_DATA_SKEW_PASS, dataSkewPass.getPassList().size());

    final Set<Class<? extends ExecutionProperty>> prerequisites = new HashSet<>();
    dataSkewPass.getPassList().forEach(compileTimePass ->
        prerequisites.addAll(compileTimePass.getPrerequisiteExecutionProperties()));
    dataSkewPass.getPassList().forEach(compileTimePass -> {
      if (compileTimePass instanceof AnnotatingPass) {
        prerequisites.removeAll(((AnnotatingPass) compileTimePass).getExecutionPropertiesToAnnotate());
      }
    });
    assertEquals(prerequisites, dataSkewPass.getPrerequisiteExecutionProperties());
  }

  /**
   * Test for {@link SkewCompositePass} with MR workload.
   * It should have inserted vertex with {@link MetricCollectTransform}
   * and vertex with {@link AggregateMetricTransform}
   * before each shuffle edge with no additional output tags.
   * @throws Exception exception on the way.
   */
  @Test
  public void testDataSkewPass() throws Exception {
    mrDAG = CompilerTestUtil.compileWordCountDAG();
    final Integer originalVerticesNum = mrDAG.getVertices().size();
    final Long numOfShuffleEdgesWithOutAdditionalOutputTag =
      mrDAG.getVertices().stream().filter(irVertex ->
        mrDAG.getIncomingEdgesOf(irVertex).stream().anyMatch(irEdge ->
          CommunicationPatternProperty.Value.Shuffle
            .equals(irEdge.getPropertyValue(CommunicationPatternProperty.class).get())
            && !irEdge.getPropertyValue(AdditionalOutputTagProperty.class).isPresent()))
      .count();
    final DAG<IRVertex, IREdge> processedDAG = new SkewCompositePass().apply(mrDAG);
    assertEquals(originalVerticesNum + numOfShuffleEdgesWithOutAdditionalOutputTag * 2,
      processedDAG.getVertices().size());

    processedDAG.filterVertices(v -> v instanceof OperatorVertex
      && ((OperatorVertex) v).getTransform() instanceof MetricCollectTransform)
      .forEach(metricV -> {
          List<IRVertex> reducerV = processedDAG.getChildren(metricV.getId());
          reducerV.forEach(rV -> assertTrue(rV.getPropertyValue(ResourceSkewedDataProperty.class).get()));
        });
  }
}
