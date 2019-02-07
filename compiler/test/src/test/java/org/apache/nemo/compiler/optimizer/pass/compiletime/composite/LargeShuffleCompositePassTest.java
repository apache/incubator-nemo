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
import org.apache.nemo.common.coder.BytesDecoderFactory;
import org.apache.nemo.common.coder.BytesEncoderFactory;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.compiler.CompilerTestUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link LargeShuffleCompositePass}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public class LargeShuffleCompositePassTest {
  private IRDAG compiledDAG;

  @Before
  public void setUp() throws Exception {
    compiledDAG = CompilerTestUtil.compileALSDAG();
  }

  @Test
  public void testLargeShuffle() {
    final IRDAG processedDAG = new LargeShuffleCompositePass().apply(compiledDAG);

    processedDAG.getTopologicalSort().forEach(irVertex -> {
      if (processedDAG.getIncomingEdgesOf(irVertex).stream().anyMatch(irEdge ->
              CommunicationPatternProperty.Value.Shuffle
          .equals(irEdge.getPropertyValue(CommunicationPatternProperty.class).get()))) {
        // Relay vertex
        processedDAG.getIncomingEdgesOf(irVertex).forEach(edgeToMerger -> {
          if (CommunicationPatternProperty.Value.Shuffle
          .equals(edgeToMerger.getPropertyValue(CommunicationPatternProperty.class).get())) {
            assertEquals(DataFlowProperty.Value.Push,
                edgeToMerger.getPropertyValue(DataFlowProperty.class).get());
            assertEquals(DataPersistenceProperty.Value.Discard,
                edgeToMerger.getPropertyValue(DataPersistenceProperty.class).get());
            assertEquals(DataStoreProperty.Value.SerializedMemoryStore,
                edgeToMerger.getPropertyValue(DataStoreProperty.class).get());
            assertEquals(BytesDecoderFactory.of(),
                edgeToMerger.getPropertyValue(DecoderProperty.class).get());
            assertEquals(CompressionProperty.Value.LZ4,
                edgeToMerger.getPropertyValue(CompressionProperty.class).get());
            assertEquals(CompressionProperty.Value.None,
                edgeToMerger.getPropertyValue(DecompressionProperty.class).get());
          } else {
            assertEquals(DataFlowProperty.Value.Pull,
                edgeToMerger.getPropertyValue(DataFlowProperty.class).get());
          }
        });
        processedDAG.getOutgoingEdgesOf(irVertex).forEach(edgeFromMerger -> {
          assertEquals(DataFlowProperty.Value.Pull,
              edgeFromMerger.getPropertyValue(DataFlowProperty.class).get());
          assertEquals(CommunicationPatternProperty.Value.OneToOne,
              edgeFromMerger.getPropertyValue(CommunicationPatternProperty.class).get());
          assertEquals(DataStoreProperty.Value.LocalFileStore,
              edgeFromMerger.getPropertyValue(DataStoreProperty.class).get());
          assertEquals(BytesEncoderFactory.of(),
              edgeFromMerger.getPropertyValue(EncoderProperty.class).get());
          assertEquals(PartitionerProperty.Type.DedicatedKeyPerElement,
              edgeFromMerger.getPropertyValue(PartitionerProperty.class).get().left());
          assertEquals(CompressionProperty.Value.None,
              edgeFromMerger.getPropertyValue(CompressionProperty.class).get());
          assertEquals(CompressionProperty.Value.LZ4,
              edgeFromMerger.getPropertyValue(DecompressionProperty.class).get());
        });
      } else {
        // Non merger vertex.
        processedDAG.getIncomingEdgesOf(irVertex).forEach(irEdge -> {
          assertEquals(DataFlowProperty.Value.Pull, irEdge.getPropertyValue(DataFlowProperty.class).get());
        });
      }
    });
  }
}
