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
package org.apache.nemo.compiler.optimizer.pass.compiletime.composite;

import org.apache.nemo.client.JobLauncher;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.executionproperty.ExecutionProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourceAntiAffinityProperty;
import org.apache.nemo.common.ir.vertex.transform.MessageAggregatorTransform;
import org.apache.nemo.common.ir.vertex.transform.TriggerTransform;
import org.apache.nemo.compiler.CompilerTestUtil;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.AnnotatingPass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.DefaultParallelismPass;
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
  private IRDAG mrDAG;

  @Before
  public void setUp() throws Exception {
  }

  /**
   * Test if the getPassList() method returns the right value upon calling it from a composite pass.
   */
  @Test
  public void testCompositePass() {
    final CompositePass dataSkewPass = new SkewCompositePass();
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
   * It should have inserted vertex with {@link TriggerTransform}
   * and vertex with {@link MessageAggregatorTransform} for each shuffle edge.
   *
   * @throws Exception exception on the way.
   */
  @Test
  public void testDataSkewPass() throws Exception {
    mrDAG = CompilerTestUtil.compileWordCountDAG();
    final Integer originalVerticesNum = mrDAG.getVertices().size();

    final Long numOfShuffleEdges =
      mrDAG.getVertices().stream().filter(irVertex ->
        mrDAG.getIncomingEdgesOf(irVertex).stream().anyMatch(irEdge ->
          CommunicationPatternProperty.Value.SHUFFLE
            .equals(irEdge.getPropertyValue(CommunicationPatternProperty.class).get())))
        .count();

    final IRDAG processedDAG = new SkewCompositePass().apply(new DefaultParallelismPass().apply(mrDAG));
    assertEquals(originalVerticesNum + numOfShuffleEdges * 2, processedDAG.getVertices().size());

    processedDAG.filterVertices(v -> v instanceof OperatorVertex
      && ((OperatorVertex) v).getTransform() instanceof TriggerTransform)
      .forEach(metricV -> {
        final List<IRVertex> reducerV = processedDAG.getChildren(metricV.getId());
        reducerV.forEach(rV -> {
          if (rV instanceof OperatorVertex &&
            !(((OperatorVertex) rV).getTransform() instanceof MessageAggregatorTransform)) {
            assertTrue(rV.getPropertyValue(ResourceAntiAffinityProperty.class).isPresent());
          }
        });
      });
  }
}
