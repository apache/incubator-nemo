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
package edu.snu.onyx.tests.compiler.optimizer.pass.compiletime.composite;

import edu.snu.onyx.client.JobLauncher;
import edu.snu.onyx.common.dag.DAG;
import edu.snu.onyx.common.ir.edge.IREdge;
import edu.snu.onyx.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.onyx.common.ir.edge.executionproperty.DataStoreProperty;
import edu.snu.onyx.common.ir.executionproperty.ExecutionProperty;
import edu.snu.onyx.common.ir.vertex.IRVertex;
import edu.snu.onyx.compiler.optimizer.pass.compiletime.composite.DisaggregationPass;
import edu.snu.onyx.compiler.optimizer.pass.compiletime.composite.InitiationCompositePass;
import edu.snu.onyx.tests.compiler.CompilerTestUtil;
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
    compiledDAG = CompilerTestUtil.compileALSDAG();
  }

  @Test
  public void testDisaggregation() throws Exception {
    final DAG<IRVertex, IREdge> processedDAG =
        new DisaggregationPass().apply(new InitiationCompositePass().apply(compiledDAG));

    processedDAG.getTopologicalSort().forEach(irVertex -> {
      processedDAG.getIncomingEdgesOf(irVertex).forEach(edgeToMerger -> {
        if (DataCommunicationPatternProperty.Value.OneToOne
              .equals(edgeToMerger.getProperty(ExecutionProperty.Key.DataCommunicationPattern))) {
          assertEquals(DataStoreProperty.Value.MemoryStore, edgeToMerger.getProperty(ExecutionProperty.Key.DataStore));
        } else {
          assertEquals(DataStoreProperty.Value.GlusterFileStore,
              edgeToMerger.getProperty(ExecutionProperty.Key.DataStore));
        }
      });
    });
  }
}
