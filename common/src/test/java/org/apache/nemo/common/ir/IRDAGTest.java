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
package org.apache.nemo.common.ir;

import org.apache.nemo.common.HashRange;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.*;
import org.apache.nemo.common.ir.vertex.utility.MessageBarrierVertex;
import org.apache.nemo.common.ir.vertex.utility.StreamVertex;
import org.apache.nemo.common.test.EmptyComponents;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import static org.junit.Assert.assertFalse;

/**
 * Tests for {@link IRDAG}.
 */
public class IRDAGTest {
  private final static int MIN_NUM_SOURCE_READABLES = 5;

  private SourceVertex sourceVertex;
  private IREdge oneToOneEdge;
  private OperatorVertex firstOperatorVertex;
  private IREdge shuffleEdge;
  private OperatorVertex secondOperatorVertex;

  private IRDAG irdag;

  @Before
  public void setUp() throws Exception {
    sourceVertex = new EmptyComponents.EmptySourceVertex("source", MIN_NUM_SOURCE_READABLES);
    firstOperatorVertex = new OperatorVertex(new EmptyComponents.EmptyTransform("first"));
    secondOperatorVertex = new OperatorVertex(new EmptyComponents.EmptyTransform("second"));

    oneToOneEdge = new IREdge(CommunicationPatternProperty.Value.OneToOne, sourceVertex, firstOperatorVertex);
    shuffleEdge = new IREdge(CommunicationPatternProperty.Value.Shuffle, firstOperatorVertex, secondOperatorVertex);

    final DAGBuilder<IRVertex, IREdge> dagBuilder = new DAGBuilder<IRVertex, IREdge>()
      .addVertex(sourceVertex)
      .addVertex(firstOperatorVertex)
      .addVertex(secondOperatorVertex)
      .connectVertices(oneToOneEdge)
      .connectVertices(shuffleEdge);
    irdag = new IRDAG(dagBuilder.build());
  }

  private void mustPass() {
    final IRDAGChecker.CheckerResult checkerResult = irdag.checkIntegrity();
    if (!checkerResult.isPassed()) {
      throw new RuntimeException("Expected pass, but failed due to ==> " + checkerResult.getFailReason());
    }
  }

  private void mustFail() {
    assertFalse(irdag.checkIntegrity().isPassed());
  }

  @Test
  public void testParallelismSuccess() {
    sourceVertex.setProperty(ParallelismProperty.of(MIN_NUM_SOURCE_READABLES));
    firstOperatorVertex.setProperty(ParallelismProperty.of(MIN_NUM_SOURCE_READABLES));
    secondOperatorVertex.setProperty(ParallelismProperty.of(2));
    shuffleEdge.setProperty(PartitionSetProperty.of(new ArrayList<>(Arrays.asList(
      HashRange.of(0, 1),
      HashRange.of(1, 2)))));
    mustPass();
  }

  @Test
  public void testParallelismSource() {
    sourceVertex.setProperty(ParallelismProperty.of(MIN_NUM_SOURCE_READABLES - 1)); // this causes failure
    firstOperatorVertex.setProperty(ParallelismProperty.of(MIN_NUM_SOURCE_READABLES - 1));
    secondOperatorVertex.setProperty(ParallelismProperty.of(MIN_NUM_SOURCE_READABLES - 1));
    mustFail();
  }

  @Test
  public void testParallelismCommPattern() {
    sourceVertex.setProperty(ParallelismProperty.of(MIN_NUM_SOURCE_READABLES));
    firstOperatorVertex.setProperty(ParallelismProperty.of(MIN_NUM_SOURCE_READABLES - 1)); // this causes failure
    secondOperatorVertex.setProperty(ParallelismProperty.of(MIN_NUM_SOURCE_READABLES - 2));
    mustFail();
  }

  @Test
  public void testPartitionSetNonShuffle() {
    oneToOneEdge.setProperty(PartitionSetProperty.of(new ArrayList<>())); // non-shuffle
    mustFail();
  }

  @Test
  public void testPartitionerNonShuffle() {
    oneToOneEdge.setProperty(PartitionerProperty.of(PartitionerProperty.Type.Hash, 2)); // non-shuffle
    mustFail();
  }

  @Test
  public void testPartitionWriteAndRead() {
    firstOperatorVertex.setProperty(ParallelismProperty.of(1));
    secondOperatorVertex.setProperty(ParallelismProperty.of(2));
    shuffleEdge.setProperty(PartitionerProperty.of(PartitionerProperty.Type.Hash, 3));
    shuffleEdge.setProperty(PartitionSetProperty.of(new ArrayList<>(Arrays.asList(
      HashRange.of(0, 2),
      HashRange.of(2, 3)))));
    mustPass();

    // This is incompatible with PartitionSet
    shuffleEdge.setProperty(PartitionerProperty.of(PartitionerProperty.Type.Hash, 2));
    mustFail();

    shuffleEdge.setProperty(PartitionSetProperty.of(new ArrayList<>(Arrays.asList(
      HashRange.of(0, 1),
      HashRange.of(1, 2)))));
    mustPass();
  }

  @Test
  public void testCompressionSymmetry() {
    oneToOneEdge.setProperty(CompressionProperty.of(CompressionProperty.Value.Gzip));
    oneToOneEdge.setProperty(DecompressionProperty.of(CompressionProperty.Value.LZ4));
    mustFail();
  }

  @Test
  public void testPartitioner() {
    // simple test case
  }

  @Test
  public void testStreamVertex() {
    final StreamVertex svOne = new StreamVertex();
    final StreamVertex svTwo = new StreamVertex();
    final StreamVertex svThree = new StreamVertex();

    irdag.insert(svOne, oneToOneEdge);
    mustPass();

    irdag.insert(svTwo, shuffleEdge);
    mustPass();

    // stream again with the new edge
    irdag.insert(svThree, shuffleEdge);
    mustPass();

    irdag.delete(svTwo);
    mustPass();

    irdag.delete(svThree);
    mustPass();

    irdag.delete(svOne);
    mustPass();

    /*
    // simple test case
    // insert
    // delete
    MessageBarrierVertex;
    StreamVertex;
    SamplingVertex;
    */
  }

  @Test
  public void testMessageBarrierVertex() {
    final MessageBarrierVertex mbOne = new MessageBarrierVertex();
    final MessageBarrierVertex mbTwo = new MessageBarrierVertex();
    final MessageBarrierVertex mbThree = new MessageBarrierVertex();

    irdag.insert(mbOne, oneToOneEdge);
    mustPass();

    irdag.insert(mbTwo, shuffleEdge);
    mustPass();

    // stream again with the new edge
    irdag.insert(mbThree, shuffleEdge);
    mustPass();

    irdag.delete(mbTwo);
    mustPass();

    irdag.delete(mbThree);
    mustPass();

    irdag.delete(mbOne);
    mustPass();

    /*
    // simple test case
    // insert
    // delete
    MessageBarrierVertex;
    StreamVertex;
    SamplingVertex;
    */
  }

  private Random random = new Random(0); // deterministic seed for reproducibility

  @Test
  public void testTenThousandRandomConfigurations() {
    // 10 thousand random configurations (some duplicate configurations possible)
    final int tenThousandConfigs = 10000;

    final List<IRVertex> insertedVertices = new ArrayList<>();
    for (int i = 0; i < tenThousandConfigs; i++) {
      final int numOfTotalMethods = 10;
      final int methodIndex = random.nextInt(numOfTotalMethods);
      switch (methodIndex) {
        // Annotation methods
        // For simplicity, we test only the EPs for which all possible values are valid.
        case 0: selectRandomVertex().setProperty(randomCSP()); break;
        case 1: selectRandomVertex().setProperty(randomRLP()); break;
        case 2: selectRandomVertex().setProperty(randomRPP()); break;
        case 3: selectRandomVertex().setProperty(randomRSP()); break;
        case 4: selectRandomEdge().setProperty(randomDFP()); break;
        case 5: selectRandomEdge().setProperty(randomDPP()); break;
        case 6: selectRandomEdge().setProperty(randomDSP()); break;

        // Reshaping methods
        case 7:
          final StreamVertex sv = new StreamVertex();
          irdag.insert(sv, selectRandomEdge());
          insertedVertices.add(sv);
          break;
        case 8:
          final MessageBarrierVertex mbv = new MessageBarrierVertex();
          irdag.insert(mbv, selectRandomEdge());
          insertedVertices.add(mbv);
          break;
        case 9: // the last index must be (numOfTotalMethods - 1)
          if (!insertedVertices.isEmpty()) {
            irdag.delete(insertedVertices.remove(random.nextInt(insertedVertices.size())));
          }
          break;
        default: throw new IllegalStateException(String.valueOf(methodIndex));
      }

      // Must pass
      mustPass();
    }
  }

  /////////////////////////// Random property generation


  private IREdge selectRandomEdge() {
    final List<IREdge> edges = irdag.getVertices().stream()
      .flatMap(v -> irdag.getIncomingEdgesOf(v).stream()).collect(Collectors.toList());
    return edges.get(random.nextInt(edges.size()));
  }

  private IRVertex selectRandomVertex() {
    return irdag.getVertices().get(random.nextInt(irdag.getVertices().size()));
  }

  ///////////////// Random vertex EP

  private ClonedSchedulingProperty randomCSP() {
    return random.nextBoolean()
      ? ClonedSchedulingProperty.of(new ClonedSchedulingProperty.CloneConf()) // upfront
      : ClonedSchedulingProperty.of(new ClonedSchedulingProperty.CloneConf(0.5, 1.5));
  }

  private ResourceLocalityProperty randomRLP() {
    return ResourceLocalityProperty.of(random.nextBoolean());
  }

  private ResourcePriorityProperty randomRPP() {
    return random.nextBoolean()
      ? ResourcePriorityProperty.of(ResourcePriorityProperty.TRANSIENT)
      : ResourcePriorityProperty.of(ResourcePriorityProperty.NONE);
  }

  private ResourceSlotProperty randomRSP() {
    return ResourceSlotProperty.of(random.nextBoolean());
  }

  ///////////////// Random edge EP

  private DataFlowProperty randomDFP() {
    return random.nextBoolean()
      ? DataFlowProperty.of(DataFlowProperty.Value.Pull)
      : DataFlowProperty.of(DataFlowProperty.Value.Push);
  }

  private DataPersistenceProperty randomDPP() {
    return random.nextBoolean()
      ? DataPersistenceProperty.of(DataPersistenceProperty.Value.Keep)
      : DataPersistenceProperty.of(DataPersistenceProperty.Value.Discard);
  }

  private DataStoreProperty randomDSP() {
    switch (random.nextInt(4)) {
      case 0: return DataStoreProperty.of(DataStoreProperty.Value.MemoryStore);
      case 1: return DataStoreProperty.of(DataStoreProperty.Value.SerializedMemoryStore);
      case 2: return DataStoreProperty.of(DataStoreProperty.Value.LocalFileStore);
      case 3: return DataStoreProperty.of(DataStoreProperty.Value.GlusterFileStore);
      default: throw new IllegalStateException();
    }
  }
}
