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
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.executionproperty.*;
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
        CommunicationPatternProperty.Value.SHUFFLE
          .equals(irEdge.getPropertyValue(CommunicationPatternProperty.class).get()))) {
        // Relay vertex
        processedDAG.getIncomingEdgesOf(irVertex).forEach(edgeToMerger -> {
          if (CommunicationPatternProperty.Value.SHUFFLE
            .equals(edgeToMerger.getPropertyValue(CommunicationPatternProperty.class).get())) {
            assertEquals(DataFlowProperty.Value.PUSH,
              edgeToMerger.getPropertyValue(DataFlowProperty.class).get());
            assertEquals(DataPersistenceProperty.Value.DISCARD,
              edgeToMerger.getPropertyValue(DataPersistenceProperty.class).get());
            assertEquals(DataStoreProperty.Value.SERIALIZED_MEMORY_FILE_STORE,
              edgeToMerger.getPropertyValue(DataStoreProperty.class).get());
            assertEquals(BytesDecoderFactory.of(),
              edgeToMerger.getPropertyValue(DecoderProperty.class).get());
            assertEquals(CompressionProperty.Value.LZ4,
              edgeToMerger.getPropertyValue(CompressionProperty.class).get());
            assertEquals(CompressionProperty.Value.NONE,
              edgeToMerger.getPropertyValue(DecompressionProperty.class).get());
          } else {
            assertEquals(DataFlowProperty.Value.PULL,
              edgeToMerger.getPropertyValue(DataFlowProperty.class).get());
          }
        });
        processedDAG.getOutgoingEdgesOf(irVertex).forEach(edgeFromMerger -> {
          assertEquals(DataFlowProperty.Value.PULL,
            edgeFromMerger.getPropertyValue(DataFlowProperty.class).get());
          assertEquals(CommunicationPatternProperty.Value.ONE_TO_ONE,
            edgeFromMerger.getPropertyValue(CommunicationPatternProperty.class).get());
          assertEquals(DataStoreProperty.Value.LOCAL_FILE_STORE,
            edgeFromMerger.getPropertyValue(DataStoreProperty.class).get());
          assertEquals(BytesEncoderFactory.of(),
            edgeFromMerger.getPropertyValue(EncoderProperty.class).get());
          assertEquals(PartitionerProperty.Type.DEDICATED_KEY_PER_ELEMENT,
            edgeFromMerger.getPropertyValue(PartitionerProperty.class).get().left());
          assertEquals(CompressionProperty.Value.NONE,
            edgeFromMerger.getPropertyValue(CompressionProperty.class).get());
          assertEquals(CompressionProperty.Value.LZ4,
            edgeFromMerger.getPropertyValue(DecompressionProperty.class).get());
        });
      }
    });
  }
}
