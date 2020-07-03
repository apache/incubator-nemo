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
import org.apache.nemo.common.dag.Edge;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.*;
import org.apache.nemo.common.ir.vertex.utility.TaskSizeSplitterVertex;
import org.apache.nemo.common.ir.vertex.utility.runtimepass.MessageAggregatorVertex;
import org.apache.nemo.common.ir.vertex.utility.runtimepass.MessageGeneratorVertex;
import org.apache.nemo.common.ir.vertex.utility.RelayVertex;
import org.apache.nemo.common.ir.vertex.utility.SamplingVertex;
import org.apache.nemo.common.ir.vertex.utility.runtimepass.SignalVertex;
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

    oneToOneEdge = new IREdge(CommunicationPatternProperty.Value.ONE_TO_ONE, sourceVertex, firstOperatorVertex);
    shuffleEdge = new IREdge(CommunicationPatternProperty.Value.SHUFFLE, firstOperatorVertex, secondOperatorVertex);

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
    oneToOneEdge.setProperty(PartitionerProperty.of(PartitionerProperty.Type.HASH, 2));
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
    shuffleEdge.setProperty(PartitionerProperty.of(PartitionerProperty.Type.HASH, 3));
    shuffleEdge.setProperty(PartitionSetProperty.of(new ArrayList<>(Arrays.asList(
      HashRange.of(0, 2),
      HashRange.of(2, 3)))));
    mustPass();

    // This is incompatible with PartitionSet
    shuffleEdge.setProperty(PartitionerProperty.of(PartitionerProperty.Type.HASH, 2));
    mustFail();

    shuffleEdge.setProperty(PartitionSetProperty.of(new ArrayList<>(Arrays.asList(
      HashRange.of(0, 1),
      HashRange.of(1, 2)))));
    mustPass();
  }

  @Test
  public void testCompressionSymmetry() {
    oneToOneEdge.setProperty(CompressionProperty.of(CompressionProperty.Value.GZIP));
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
    oneToOneEdge.setProperty(DataFlowProperty.of(DataFlowProperty.Value.PULL));
    firstOperatorVertex.setProperty(ScheduleGroupProperty.of(1)); // not split by Pull - failure
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
  public void testSignalVertex() {
    final SignalVertex sg1 = insertNewSignalVertex(irdag, oneToOneEdge);
    mustPass();

    final SignalVertex sg2 = insertNewSignalVertex(irdag, shuffleEdge);
    mustPass();

    irdag.delete(sg1);
    mustPass();

    irdag.delete(sg2);
    mustPass();
  }

  @Test
  public void testSamplingVertex() {
    final SamplingVertex svOne = new SamplingVertex(sourceVertex, 0.1f);
    irdag.insert(Sets.newHashSet(svOne), Sets.newHashSet(sourceVertex));
    mustPass();

    final SamplingVertex svTwo = new SamplingVertex(firstOperatorVertex, 0.1f);
    irdag.insert(Sets.newHashSet(svTwo), Sets.newHashSet(firstOperatorVertex));
    mustPass();

    irdag.delete(svTwo);
    mustPass();

    irdag.delete(svOne);
    mustPass();
  }

  @Test
  public void testSplitterVertex() {
    final TaskSizeSplitterVertex sp = new TaskSizeSplitterVertex(
      "splitter_1",
      Sets.newHashSet(secondOperatorVertex),
      Sets.newHashSet(secondOperatorVertex),
      Sets.newHashSet(),
      Sets.newHashSet(secondOperatorVertex),
      Sets.newHashSet(),
      1024
    );
    irdag.insert(sp);
    mustPass();

    irdag.delete(sp);
    mustPass();
  }

  private MessageAggregatorVertex insertNewTriggerVertex(final IRDAG dag, final IREdge edgeToGetStatisticsOf) {
    final MessageGeneratorVertex mb = new MessageGeneratorVertex<>((l, r) -> null);
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

  private Optional<TaskSizeSplitterVertex> insertNewSplitterVertex(final IRDAG dag,
                                                                   final IREdge edgeToSplitterVertex) {
    final Set<IRVertex> vertexGroup = getVertexGroupToInsertSplitter(irdag, edgeToSplitterVertex);
    if (vertexGroup.isEmpty()) {
      return Optional.empty();
    }
    Set<IRVertex> verticesWithGroupOutgoingEdges = new HashSet<>();
    for (IRVertex vertex : vertexGroup) {
      Set<IRVertex> nextVertices = irdag.getOutgoingEdgesOf(vertex).stream().map(Edge::getDst)
        .collect(Collectors.toSet());
      for (IRVertex nextVertex : nextVertices) {
        if (!vertexGroup.contains(nextVertex)) {
          verticesWithGroupOutgoingEdges.add(vertex);
        }
      }
    }
    Set<IRVertex> groupEndingVertices = vertexGroup.stream()
      .filter(stageVertex -> irdag.getOutgoingEdgesOf(stageVertex).isEmpty()
        || !irdag.getOutgoingEdgesOf(stageVertex).stream().map(Edge::getDst).anyMatch(vertexGroup::contains))
      .collect(Collectors.toSet());

    final Set<IREdge> edgesBetweenOriginalVertices = vertexGroup
      .stream()
      .flatMap(ov -> dag.getIncomingEdgesOf(ov).stream())
      .filter(edge -> vertexGroup.contains(edge.getSrc()))
      .collect(Collectors.toSet());

    TaskSizeSplitterVertex sp = new TaskSizeSplitterVertex(
      "sp" + edgeToSplitterVertex.getId(),
      vertexGroup,
      Sets.newHashSet(edgeToSplitterVertex.getDst()),
      verticesWithGroupOutgoingEdges,
      groupEndingVertices,
      edgesBetweenOriginalVertices,
      1024);

    dag.insert(sp);

    return Optional.of(sp);
  }

  private SignalVertex insertNewSignalVertex(final IRDAG dag, final IREdge edgeToOptimize) {
    final SignalVertex sg = new SignalVertex();
    dag.insert(sg, edgeToOptimize);
    return sg;
  }

  ////////////////////////////////////////////////////// Random generative tests

  private Random random = new Random(0); // deterministic seed for reproducibility

  @Test
  public void testThousandRandomConfigurations() {
    // Thousand random configurations (some duplicate configurations possible)
    final int thousandConfigs = 1000;
    for (int i = 0; i < thousandConfigs; i++) {
      //LOG.info("Doing {}", i);
      final int numOfTotalMethods = 13;
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
          insertNewSignalVertex(irdag, selectRandomEdge());
          break;
        case 11:
          insertNewSplitterVertex(irdag, selectRandomEdge());
          break;
        case 12:
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
        //irdag.storeJSON("test_reshaping_snapshots", i + "(methodIndex_" + methodIndex + ")", "test");
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

  /**
   * Private helper method to check if the parameter observingEdge is appropriate for inserting Splitter Vertex.
   * Specifically, this edge is considered to be the incoming edge of splitter vertex.
   * This edge should have communication property of shuffle, and should be the only edge coming out from/coming in to
   * its source/dest.
   * @param dag           dag to observe.
   * @param observingEdge observing edge.
   * @return              true if this edge is appropriate for inserting splitter vertex.
   */
  private boolean isThisEdgeAppropriateForInsertingSplitterVertex(IRDAG dag, IREdge observingEdge) {
    // If communication property of observing Edge is not shuffle, return false.
    if (!CommunicationPatternProperty.Value.SHUFFLE.equals(
      observingEdge.getPropertyValue(CommunicationPatternProperty.class).get())) {
      return false;
    }

    // If destination of observingEdge has multiple incoming edges, return false.
    if (dag.getIncomingEdgesOf(observingEdge.getDst()).size() > 1) {
      return false;
    }

    // If source of observingEdge has multiple outgoing edges, return false.
    if (dag.getOutgoingEdgesOf(observingEdge.getSrc()).size() > 1) {
      return false;
    }
    return true;
  }

  private Set<IRVertex> getVertexGroupToInsertSplitter(IRDAG dag, IREdge observingEdge) {
    final Set<IRVertex> vertexGroup = new HashSet<>();

    // If this edge is not appropriate to be the incoming edge of splitter vertex, return empty set.
    if (!isThisEdgeAppropriateForInsertingSplitterVertex(dag, observingEdge)) {
      return new HashSet<>();
    }

    if (observingEdge.getDst() instanceof MessageGeneratorVertex
      || observingEdge.getDst() instanceof MessageAggregatorVertex) {
      return new HashSet<>();
    }
    // Get the vertex group.
    vertexGroup.add(observingEdge.getDst());
    for (IREdge edge : dag.getOutgoingEdgesOf(observingEdge.getDst())) {
      vertexGroup.addAll(recursivelyAddVertexGroup(dag, edge, vertexGroup));
    }

    // Check if this vertex group is appropriate for inserting splitter vertex
    Set<IREdge> stageOutgoingEdges = vertexGroup
      .stream()
      .flatMap(vertex -> dag.getOutgoingEdgesOf(vertex).stream())
      .filter(edge -> !vertexGroup.contains(edge.getDst()))
      .collect(Collectors.toSet());
    if (stageOutgoingEdges.isEmpty()) {
      return vertexGroup;
    } else {
      for (IREdge edge : stageOutgoingEdges) {
        if (CommunicationPatternProperty.Value.ONE_TO_ONE.equals(
          edge.getPropertyValue(CommunicationPatternProperty.class).get())) {
          return new HashSet<>();
        }
      }
    }
    return vertexGroup;
  }

  /**
   * Check of the destination of the observing edge can be added in vertex group.
   * @param dag             dag to observe.
   * @param observingEdge   edge to observe.
   * @param vertexGroup     vertex group to add.
   * @return                updated vertex group.
   */
  private Set<IRVertex> recursivelyAddVertexGroup(IRDAG dag, IREdge observingEdge, Set<IRVertex> vertexGroup) {
    // do not update.
    if (dag.getIncomingEdgesOf(observingEdge.getDst()).size() > 1) {
      return vertexGroup;
    }
    // do not update.
    if (observingEdge.getPropertyValue(CommunicationPatternProperty.class).orElseThrow(IllegalStateException::new)
      != CommunicationPatternProperty.Value.ONE_TO_ONE) {
      return vertexGroup;
    }
    // do not update.
    if (!observingEdge.getSrc().getExecutionProperties().equals(observingEdge.getDst().getExecutionProperties())) {
      return vertexGroup;
    }
    // do not update.
    if (observingEdge.getDst() instanceof MessageGeneratorVertex
      || observingEdge.getDst() instanceof MessageAggregatorVertex) {
      return vertexGroup;
    }
    // do update.
    vertexGroup.add(observingEdge.getDst());
    for (IREdge edge : dag.getOutgoingEdgesOf(observingEdge.getDst())) {
     vertexGroup.addAll(recursivelyAddVertexGroup(dag, edge, vertexGroup));
    }
    return vertexGroup;
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
      ? DataFlowProperty.of(DataFlowProperty.Value.PULL)
      : DataFlowProperty.of(DataFlowProperty.Value.PUSH);
  }

  private DataPersistenceProperty randomDPP() {
    return random.nextBoolean()
      ? DataPersistenceProperty.of(DataPersistenceProperty.Value.KEEP)
      : DataPersistenceProperty.of(DataPersistenceProperty.Value.DISCARD);
  }

  private DataStoreProperty randomDSP() {
    switch (random.nextInt(4)) {
      case 0:
        return DataStoreProperty.of(DataStoreProperty.Value.MEMORY_STORE);
      case 1:
        return DataStoreProperty.of(DataStoreProperty.Value.SERIALIZED_MEMORY_STORE);
      case 2:
        return DataStoreProperty.of(DataStoreProperty.Value.LOCAL_FILE_STORE);
      case 3:
        return DataStoreProperty.of(DataStoreProperty.Value.GLUSTER_FILE_STORE);
      default:
        throw new IllegalStateException();
    }
  }
}
