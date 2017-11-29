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
package edu.snu.onyx.tests.compiler.optimizer.pass.compiletime.annotating;

import edu.snu.onyx.client.JobLauncher;
import edu.snu.onyx.common.dag.DAG;
import edu.snu.onyx.common.ir.edge.IREdge;
import edu.snu.onyx.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.onyx.common.ir.edge.executionproperty.PartitionerProperty;
import edu.snu.onyx.common.ir.executionproperty.ExecutionProperty;
import edu.snu.onyx.common.ir.vertex.IRVertex;
import edu.snu.onyx.compiler.optimizer.pass.compiletime.annotating.AnnotatingPass;
import edu.snu.onyx.compiler.optimizer.pass.compiletime.annotating.DefaultPartitionerPass;
import edu.snu.onyx.tests.compiler.CompilerTestUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link DefaultPartitionerPass}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public class DefaultPartitionerPassTest {
  private AnnotatingPass partitionerPass;

  @Before
  public void setUp() throws Exception {
    partitionerPass = new DefaultPartitionerPass();
  }

  @Test
  public void testAnnotatingProperty() {
    assertEquals(ExecutionProperty.Key.Partitioner, partitionerPass.getExecutionPropertyToModify());
  }

  @Test
  public void testAnnotation() throws Exception {
    final DAG<IRVertex, IREdge> compiledDAG = CompilerTestUtil.compileALSDAG();
    final DAG<IRVertex, IREdge> processedDAG = partitionerPass.apply(compiledDAG);

    processedDAG.getVertices().forEach(v -> processedDAG.getIncomingEdgesOf(v).stream()
        .filter(e -> e.getProperty(ExecutionProperty.Key.DataCommunicationPattern)
                      .equals(DataCommunicationPatternProperty.Value.ScatterGather))
        .forEach(e -> assertEquals(e.getProperty(ExecutionProperty.Key.Partitioner),
            PartitionerProperty.Value.HashPartitioner)));

    processedDAG.getVertices().forEach(v -> processedDAG.getIncomingEdgesOf(v).stream()
        .filter(e -> !e.getProperty(ExecutionProperty.Key.DataCommunicationPattern)
                        .equals(DataCommunicationPatternProperty.Value.ScatterGather))
        .forEach(e -> assertEquals(e.getProperty(ExecutionProperty.Key.Partitioner),
            PartitionerProperty.Value.IntactPartitioner)));
  }
}
