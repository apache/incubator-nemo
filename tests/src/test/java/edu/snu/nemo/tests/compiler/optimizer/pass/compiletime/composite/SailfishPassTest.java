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
package edu.snu.nemo.tests.compiler.optimizer.pass.compiletime.composite;

import edu.snu.nemo.client.JobLauncher;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.DataFlowModelProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.UsedDataHandlingProperty;
import edu.snu.nemo.common.ir.executionproperty.ExecutionProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.composite.SailfishPass;
import edu.snu.nemo.tests.compiler.CompilerTestUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link SailfishPass}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public class SailfishPassTest {
  private DAG<IRVertex, IREdge> compiledDAG;

  @Before
  public void setUp() throws Exception {
    compiledDAG = CompilerTestUtil.compileALSDAG();
  }

  @Test
  public void testSailfish() throws Exception {
    final DAG<IRVertex, IREdge> processedDAG = new SailfishPass().apply(compiledDAG);

    processedDAG.getTopologicalSort().forEach(irVertex -> {
      if (processedDAG.getIncomingEdgesOf(irVertex).stream().anyMatch(irEdge ->
              DataCommunicationPatternProperty.Value.Shuffle
          .equals(irEdge.getProperty(ExecutionProperty.Key.DataCommunicationPattern)))) {
        // Merger vertex
        processedDAG.getIncomingEdgesOf(irVertex).forEach(edgeToMerger -> {
          if (DataCommunicationPatternProperty.Value.Shuffle
          .equals(edgeToMerger.getProperty(ExecutionProperty.Key.DataCommunicationPattern))) {
            assertEquals(DataFlowModelProperty.Value.Push,
                edgeToMerger.getProperty(ExecutionProperty.Key.DataFlowModel));
            assertEquals(UsedDataHandlingProperty.Value.Discard,
                edgeToMerger.getProperty(ExecutionProperty.Key.UsedDataHandling));
            assertEquals(DataStoreProperty.Value.SerializedMemoryStore,
                edgeToMerger.getProperty(ExecutionProperty.Key.DataStore));
          } else {
            assertEquals(DataFlowModelProperty.Value.Pull,
                edgeToMerger.getProperty(ExecutionProperty.Key.DataFlowModel));
          }
        });
        processedDAG.getOutgoingEdgesOf(irVertex).forEach(edgeFromMerger -> {
          assertEquals(DataFlowModelProperty.Value.Pull,
              edgeFromMerger.getProperty(ExecutionProperty.Key.DataFlowModel));
          assertEquals(DataCommunicationPatternProperty.Value.OneToOne,
              edgeFromMerger.getProperty(ExecutionProperty.Key.DataCommunicationPattern));
          assertEquals(DataStoreProperty.Value.LocalFileStore,
              edgeFromMerger.getProperty(ExecutionProperty.Key.DataStore));
        });
      } else {
        // Non merger vertex.
        processedDAG.getIncomingEdgesOf(irVertex).forEach(irEdge -> {
          assertEquals(DataFlowModelProperty.Value.Pull, irEdge.getProperty(ExecutionProperty.Key.DataFlowModel));
        });
      }
    });
  }
}
