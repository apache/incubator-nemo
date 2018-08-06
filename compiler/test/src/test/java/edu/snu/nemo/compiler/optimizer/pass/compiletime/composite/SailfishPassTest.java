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
import edu.snu.nemo.common.coder.BytesDecoderFactory;
import edu.snu.nemo.common.coder.BytesEncoderFactory;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.*;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.compiler.CompilerTestUtil;
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
  public void testSailfish() {
    final DAG<IRVertex, IREdge> processedDAG = new SailfishPass().apply(compiledDAG);

    processedDAG.getTopologicalSort().forEach(irVertex -> {
      if (processedDAG.getIncomingEdgesOf(irVertex).stream().anyMatch(irEdge ->
              DataCommunicationPatternProperty.Value.Shuffle
          .equals(irEdge.getPropertyValue(DataCommunicationPatternProperty.class).get()))) {
        // Relay vertex
        processedDAG.getIncomingEdgesOf(irVertex).forEach(edgeToMerger -> {
          if (DataCommunicationPatternProperty.Value.Shuffle
          .equals(edgeToMerger.getPropertyValue(DataCommunicationPatternProperty.class).get())) {
            assertEquals(DataFlowModelProperty.Value.Push,
                edgeToMerger.getPropertyValue(DataFlowModelProperty.class).get());
            assertEquals(UsedDataHandlingProperty.Value.Discard,
                edgeToMerger.getPropertyValue(UsedDataHandlingProperty.class).get());
            assertEquals(InterTaskDataStoreProperty.Value.SerializedMemoryStore,
                edgeToMerger.getPropertyValue(InterTaskDataStoreProperty.class).get());
            assertEquals(BytesDecoderFactory.of(),
                edgeToMerger.getPropertyValue(DecoderProperty.class).get());
            assertEquals(CompressionProperty.Value.LZ4,
                edgeToMerger.getPropertyValue(CompressionProperty.class).get());
            assertEquals(CompressionProperty.Value.None,
                edgeToMerger.getPropertyValue(DecompressionProperty.class).get());
          } else {
            assertEquals(DataFlowModelProperty.Value.Pull,
                edgeToMerger.getPropertyValue(DataFlowModelProperty.class).get());
          }
        });
        processedDAG.getOutgoingEdgesOf(irVertex).forEach(edgeFromMerger -> {
          assertEquals(DataFlowModelProperty.Value.Pull,
              edgeFromMerger.getPropertyValue(DataFlowModelProperty.class).get());
          assertEquals(DataCommunicationPatternProperty.Value.OneToOne,
              edgeFromMerger.getPropertyValue(DataCommunicationPatternProperty.class).get());
          assertEquals(InterTaskDataStoreProperty.Value.LocalFileStore,
              edgeFromMerger.getPropertyValue(InterTaskDataStoreProperty.class).get());
          assertEquals(BytesEncoderFactory.of(),
              edgeFromMerger.getPropertyValue(EncoderProperty.class).get());
          assertEquals(PartitionerProperty.Value.IncrementPartitioner,
              edgeFromMerger.getPropertyValue(PartitionerProperty.class).get());
          assertEquals(CompressionProperty.Value.None,
              edgeFromMerger.getPropertyValue(CompressionProperty.class).get());
          assertEquals(CompressionProperty.Value.LZ4,
              edgeFromMerger.getPropertyValue(DecompressionProperty.class).get());
        });
      } else {
        // Non merger vertex.
        processedDAG.getIncomingEdgesOf(irVertex).forEach(irEdge -> {
          assertEquals(DataFlowModelProperty.Value.Pull, irEdge.getPropertyValue(DataFlowModelProperty.class).get());
        });
      }
    });
  }
}
