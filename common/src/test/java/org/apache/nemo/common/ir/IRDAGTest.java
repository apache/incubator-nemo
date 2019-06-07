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

import com.google.common.collect.Sets;
import org.apache.nemo.common.HashRange;
import org.apache.nemo.common.Util;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.*;
import org.apache.nemo.common.ir.vertex.utility.MessageAggregatorVertex;
import org.apache.nemo.common.ir.vertex.utility.TriggerVertex;
import org.apache.nemo.common.ir.vertex.utility.RelayVertex;
import org.apache.nemo.common.ir.vertex.utility.SamplingVertex;
import org.apache.nemo.common.test.EmptyComponents;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertFalse;

/**
 * Tests for {@link IRDAG}.
 */
public class IRDAGTest {
  private static final Logger LOG = LoggerFactory.getLogger(IRDAG.class.getName());

  private final static int MIN_THREE_SOURCE_READABLES = 3;

  private SourceVertex sourceVertex;
  private IREdge oneToOneEdge;
  private OperatorVertex firstOperatorVertex;
  private IREdge shuffleEdge;
  private OperatorVertex secondOperatorVertex;

  private IRDAG irdag;

  @Before
  public void setUp() throws Exception {
    sourceVertex = new EmptyComponents.EmptySourceVertex("source", MIN_THREE_SOURCE_READABLES);
    firstOperatorVertex = new OperatorVertex(new EmptyComponents.EmptyTransform("first"));
    secondOperatorVertex = new OperatorVertex(new EmptyComponents.EmptyTransform("second"));

    oneToOneEdge = new IREdge(CommunicationPatternProperty.Value.OneToOne, sourceVertex, firstOperatorVertex);
    shuffleEdge = new IREdge(CommunicationPatternProperty.Value.Shuffle, firstOperatorVertex, secondOperatorVertex);

    // To pass the key-related checkers
    shuffleEdge.setProperty(KeyDecoderProperty.of(DecoderFactory.DUMMY_DECODER_FACTORY));
    shuffleEdge.setProperty(KeyEncoderProperty.of(EncoderFactory.DUMMY_ENCODER_FACTORY));
    shuffleEdge.setProperty(KeyExtractorProperty.of(element -> null));

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
      irdag.storeJSON("debug", "mustPass() failure", "integrity failure");
      throw new RuntimeException("(See [debug] folder for visualization) " +
        "Expected pass, but failed due to ==> " + checkerResult.getFailReason());
    }
  }

  private void mustFail() {
    assertFalse(irdag.checkIntegrity().isPassed());
  }

  @Test
  public void testParallelismSuccess() {
    sourceVertex.setProperty(ParallelismProperty.of(MIN_THREE_SOURCE_READABLES));
    firstOperatorVertex.setProperty(ParallelismProperty.of(MIN_THREE_SOURCE_READABLES));
    secondOperatorVertex.setProperty(ParallelismProperty.of(2));
    shuffleEdge.setProperty(PartitionSetProperty.of(new ArrayList<>(Arrays.asList(
      HashRange.of(0, 1),
      HashRange.of(1, 2)))));
    mustPass();
  }

  @Test
  public void testParallelismSource() {
    sourceVertex.setProperty(ParallelismProperty.of(MIN_THREE_SOURCE_READABLES - 1)); // smaller than min - fail
    firstOperatorVertex.setProperty(ParallelismProperty.of(MIN_THREE_SOURCE_READABLES - 1));
    secondOperatorVertex.setProperty(ParallelismProperty.of(MIN_THREE_SOURCE_READABLES - 1));

    mustFail();
  }

  @Test
  public void testParallelismCommPattern() {
    sourceVertex.setProperty(ParallelismProperty.of(MIN_THREE_SOURCE_READABLES));
    firstOperatorVertex.setProperty(ParallelismProperty.of(MIN_THREE_SOURCE_READABLES - 1)); // smaller than o2o - fail
    secondOperatorVertex.setProperty(ParallelismProperty.of(MIN_THREE_SOURCE_READABLES - 2));

    mustFail();
  }

  @Test
  public void testParallelismPartitionSet() {
    sourceVertex.setProperty(ParallelismProperty.of(MIN_THREE_SOURCE_READABLES));
    firstOperatorVertex.setProperty(ParallelismProperty.of(MIN_THREE_SOURCE_READABLES));
    secondOperatorVertex.setProperty(ParallelismProperty.of(MIN_THREE_SOURCE_READABLES));

    // this causes failure (only 2 KeyRanges < 3 parallelism)
    shuffleEdge.setProperty(PartitionSetProperty.of(new ArrayList<>(Arrays.asList(
      HashRange.of(0, 1),
      HashRange.of(1, 2)
    ))));
  }

  @Test
  public void testPartitionSetNonShuffle() {
    oneToOneEdge.setProperty(PartitionSetProperty.of(new ArrayList<>())); // non-shuffle - fail
    mustFail();
  }

  @Test
  public void testPartitionerNonShuffle() {
    // non-shuffle - fail
    oneToOneEdge.setProperty(PartitionerProperty.of(PartitionerProperty.Type.Hash, 2));
    mustFail();
  }

  @Test
  public void testParallelismResourceSite() {
    sourceVertex.setProperty(ParallelismProperty.of(MIN_THREE_SOURCE_READABLES));
    firstOperatorVertex.setProperty(ParallelismProperty.of(MIN_THREE_SOURCE_READABLES));
    secondOperatorVertex.setProperty(ParallelismProperty.of(MIN_THREE_SOURCE_READABLES));

    // must pass
    final HashMap<String, Integer> goodSite = new HashMap<>();
    goodSite.put("SiteA", 1);
    goodSite.put("SiteB", MIN_THREE_SOURCE_READABLES - 1);
    firstOperatorVertex.setProperty(ResourceSiteProperty.of(goodSite));
    mustPass();

    // must fail
    final HashMap<String, Integer> badSite = new HashMap<>();
    badSite.put("SiteA", 1);
    badSite.put("SiteB", MIN_THREE_SOURCE_READABLES - 2); // sum is smaller than parallelism
    firstOperatorVertex.setProperty(ResourceSiteProperty.of(badSite));
    mustFail();
  }

  @Test
  public void testParallelismResourceAntiAffinity() {
    sourceVertex.setProperty(ParallelismProperty.of(MIN_THREE_SOURCE_READABLES));
    firstOperatorVertex.setProperty(ParallelismProperty.of(MIN_THREE_SOURCE_READABLES));
    secondOperatorVertex.setProperty(ParallelismProperty.of(MIN_THREE_SOURCE_READABLES));

    // must pass
    final HashSet<Integer> goodSet = new HashSet<>();
    goodSet.add(0);
    goodSet.add(MIN_THREE_SOURCE_READABLES - 1);
    firstOperatorVertex.setProperty(ResourceAntiAffinityProperty.of(goodSet));
    mustPass();

    // must fail
    final HashSet<Integer> badSet = new HashSet<>();
    badSet.add(MIN_THREE_SOURCE_READABLES + 1); // ofset out of range - fail
    firstOperatorVertex.setProperty(ResourceAntiAffinityProperty.of(badSet));
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
    oneToOneEdge.setProperty(DecompressionProperty.of(CompressionProperty.Value.LZ4)); // not symmetric - failure
    mustFail();
  }

  @Test
  public void testScheduleGroupOrdering() {
    sourceVertex.setProperty(ScheduleGroupProperty.of(1));
    firstOperatorVertex.setProperty(ScheduleGroupProperty.of(2));
    secondOperatorVertex.setProperty(ScheduleGroupProperty.of(1)); // decreases - failure
    mustFail();
  }

  @Test
  public void testScheduleGroupPull() {
    sourceVertex.setProperty(ScheduleGroupProperty.of(1));
    oneToOneEdge.setProperty(DataFlowProperty.of(DataFlowProperty.Value.Pull));
    firstOperatorVertex.setProperty(ScheduleGroupProperty.of(1)); // not split by PULL - failure
    mustFail();
  }

  @Test
  public void testCache() {
    oneToOneEdge.setProperty(CacheIDProperty.of(UUID.randomUUID()));
    mustFail(); // need a cache marker vertex - failure
  }

  @Test
  public void testStreamVertex() {
    final RelayVertex svOne = new RelayVertex();
    irdag.insert(svOne, oneToOneEdge);
    mustPass();

    final RelayVertex svTwo = new RelayVertex();
    irdag.insert(svTwo, shuffleEdge);
    mustPass();

    irdag.delete(svTwo);
    mustPass();

    irdag.delete(svOne);
    mustPass();
  }

  @Test
  public void testTriggerVertex() {
    final MessageAggregatorVertex maOne = insertNewTriggerVertex(irdag, oneToOneEdge);
    mustPass();

    final MessageAggregatorVertex maTwo = insertNewTriggerVertex(irdag, shuffleEdge);
    mustPass();

    irdag.delete(maTwo);
    mustPass();

    irdag.delete(maOne);
    mustPass();
  }

  @Test
  public void testSamplingVertex() {
    final SamplingVertex svOne = new SamplingVertex(sourceVertex, 0.1f);
    irdag.insert(Sets.newHashSet(svOne), Sets.newHashSet(sourceVertex));
    mustPass();

    final SamplingVertex svTwo = new SamplingVertex(firstOperatorVertex, 0.1f);
    ;
    irdag.insert(Sets.newHashSet(svTwo), Sets.newHashSet(firstOperatorVertex));
    mustPass();

    irdag.delete(svTwo);
    mustPass();

    irdag.delete(svOne);
    mustPass();
  }

  private MessageAggregatorVertex insertNewTriggerVertex(final IRDAG dag, final IREdge edgeToGetStatisticsOf) {
    final TriggerVertex mb = new TriggerVertex<>((l, r) -> null);
    final MessageAggregatorVertex ma = new MessageAggregatorVertex<>(() -> new Object(), (l, r) -> null);
    dag.insert(
      mb,
      ma,
      EncoderProperty.of(EncoderFactory.DUMMY_ENCODER_FACTORY),
      DecoderProperty.of(DecoderFactory.DUMMY_DECODER_FACTORY),
      Sets.newHashSet(edgeToGetStatisticsOf),
      Sets.newHashSet(edgeToGetStatisticsOf));
    return ma;
  }

  ////////////////////////////////////////////////////// Random generative tests

  private Random random = new Random(0); // deterministic seed for reproducibility

  @Test
  public void testThousandRandomConfigurations() {
    // Thousand random configurations (some duplicate configurations possible)
    final int thousandConfigs = 1000;
    for (int i = 0; i < thousandConfigs; i++) {
      // LOG.info("Doing {}", i);
      final int numOfTotalMethods = 11;
      final int methodIndex = random.nextInt(numOfTotalMethods);
      switch (methodIndex) {
        // Annotation methods
        // For simplicity, we test only the EPs for which all possible values are valid.
        case 0:
          selectRandomVertex().setProperty(randomCSP());
          break;
        case 1:
          selectRandomVertex().setProperty(randomRLP());
          break;
        case 2:
          selectRandomVertex().setProperty(randomRPP());
          break;
        case 3:
          selectRandomVertex().setProperty(randomRSP());
          break;
        case 4:
          selectRandomEdge().setProperty(randomDFP());
          break;
        case 5:
          selectRandomEdge().setProperty(randomDPP());
          break;
        case 6:
          selectRandomEdge().setProperty(randomDSP());
          break;

        // Reshaping methods
        case 7:
          final RelayVertex relayVertex = new RelayVertex();
          final IREdge edgeToStreamize = selectRandomEdge();
          if (!(edgeToStreamize.getPropertyValue(MessageIdEdgeProperty.class).isPresent()
            && !edgeToStreamize.getPropertyValue(MessageIdEdgeProperty.class).get().isEmpty())) {
            irdag.insert(relayVertex, edgeToStreamize);
          }
          break;
        case 8:
          insertNewTriggerVertex(irdag, selectRandomEdge());
          break;
        case 9:
          final IRVertex vertexToSample = selectRandomNonUtilityVertex();
          final SamplingVertex samplingVertex = new SamplingVertex(vertexToSample, 0.1f);
          irdag.insert(Sets.newHashSet(samplingVertex), Sets.newHashSet(vertexToSample));
          break;
        case 10:
          // the last index must be (numOfTotalMethods - 1)
          selectRandomUtilityVertex().ifPresent(irdag::delete);
          break;
        default:
          throw new IllegalStateException(String.valueOf(methodIndex));
      }

      if (i % (thousandConfigs / 10) == 0) {
        // Uncomment to visualize 10 DAG snapshots
        // irdag.storeJSON("test_10_snapshots", String.valueOf(i), "test");
      }

      if (methodIndex >= 7) {
        // Uncomment to visualize DAG snapshots after reshaping (insert, delete)
        // irdag.storeJSON("test_reshaping_snapshots", i + "(methodIndex_" + methodIndex + ")", "test");
      }

      // Must always pass
      mustPass();
    }
  }

  private IREdge selectRandomEdge() {
    final List<IREdge> edges = irdag.getVertices().stream()
      .flatMap(v -> irdag.getIncomingEdgesOf(v).stream()).collect(Collectors.toList());
    while (true) {
      final IREdge selectedEdge = edges.get(random.nextInt(edges.size()));
      if (!Util.isControlEdge(selectedEdge)) {
        return selectedEdge;
      }
    }
  }

  private IRVertex selectRandomVertex() {
    return irdag.getVertices().get(random.nextInt(irdag.getVertices().size()));
  }

  private IRVertex selectRandomNonUtilityVertex() {
    final List<IRVertex> nonUtilityVertices =
      irdag.getVertices().stream().filter(v -> !Util.isUtilityVertex(v)).collect(Collectors.toList());
    return nonUtilityVertices.get(random.nextInt(nonUtilityVertices.size()));
  }

  private Optional<IRVertex> selectRandomUtilityVertex() {
    final List<IRVertex> utilityVertices =
      irdag.getVertices().stream().filter(Util::isUtilityVertex).collect(Collectors.toList());
    return utilityVertices.isEmpty()
      ? Optional.empty()
      : Optional.of(utilityVertices.get(random.nextInt(utilityVertices.size())));
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
      case 0:
        return DataStoreProperty.of(DataStoreProperty.Value.MemoryStore);
      case 1:
        return DataStoreProperty.of(DataStoreProperty.Value.SerializedMemoryStore);
      case 2:
        return DataStoreProperty.of(DataStoreProperty.Value.LocalFileStore);
      case 3:
        return DataStoreProperty.of(DataStoreProperty.Value.GlusterFileStore);
      default:
        throw new IllegalStateException();
    }
  }
}
