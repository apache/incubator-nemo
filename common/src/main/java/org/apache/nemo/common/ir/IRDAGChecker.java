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

import org.apache.nemo.common.KeyRange;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;
import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.*;
import org.apache.nemo.common.ir.vertex.utility.StreamVertex;
import org.apache.nemo.common.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Checks the integrity of an IR DAG.
 */
public class IRDAGChecker {
  private static final Logger LOG = LoggerFactory.getLogger(IRDAGChecker.class.getName());

  private static final IRDAGChecker SINGLETON = new IRDAGChecker();

  private final List<SingleVertexChecker> singleVertexCheckerList;
  private final List<SingleEdgeChecker> singleEdgeCheckerList;
  private final List<NeighborChecker> neighborCheckerList;
  private final List<GlobalDAGChecker> globalDAGCheckerList;

  public static IRDAGChecker get() {
    return SINGLETON;
  }

  private IRDAGChecker() {
    this.singleVertexCheckerList = new ArrayList<>();
    this.singleEdgeCheckerList = new ArrayList<>();
    this.neighborCheckerList = new ArrayList<>();
    this.globalDAGCheckerList = new ArrayList<>();

    addParallelismCheckers();
    addShuffleEdgeCheckers();
    addPartitioningCheckers();
    addEncodingCompressionCheckers();
    addMessageBarrierCheckers();
    addStreamVertexCheckers();
  }

  ///////////////////////////// Checker interfaces

  /**
   * Checks each single vertex.
   */
  private interface SingleVertexChecker {
    CheckerResult check(final IRVertex irVertex);
  }

  /**
   * Checks each single edge.
   */
  private interface SingleEdgeChecker {
    CheckerResult check(final IREdge irEdge);
  }

  /**
   * Checks each vertex and its neighbor edges.
   */
  private interface NeighborChecker {
    CheckerResult check(final IRVertex irVertex,
                        final List<IREdge> inEdges,
                        final List<IREdge> outEdges);
  }

  /**
   * Checks the entire DAG.
   */
  public interface GlobalDAGChecker {
    CheckerResult check(final DAG<IRVertex, IREdge> irdag);
  }

  ///////////////////////////// Set up

  private Set<Integer> getZeroToNSet(final int n) {
    return IntStream.range(0, n)
      .boxed()
      .collect(Collectors.toSet());
  }

  /**
   * Parallelism-related checkers.
   */
  void addParallelismCheckers() {
    final SingleVertexChecker parallelismWithOtherEPsInSingleVertex = (v -> {
      final Optional<Integer> parallelism = v.getPropertyValue(ParallelismProperty.class);
      if (!parallelism.isPresent()) {
        return success(); // No need to check, if the parallelism is not set yet
      }

      final Optional<Integer> resourceSiteSize = v.getPropertyValue(ResourceSiteProperty.class)
        .map(rs -> rs.values().stream().mapToInt(Integer::intValue).sum());
      if (resourceSiteSize.isPresent() && !parallelism.equals(resourceSiteSize)) {
        return failure("Parallelism must equal to sum of site nums",
          v, ParallelismProperty.class, ResourceSiteProperty.class);
      }

      final Optional<HashSet<Integer>> antiAffinitySet = v.getPropertyValue(ResourceAntiAffinityProperty.class);
      if (antiAffinitySet.isPresent()
        && !getZeroToNSet(parallelism.get()).containsAll(antiAffinitySet.get())) {
        return failure("Offsets must be within parallelism",
          v, ParallelismProperty.class, ResourceAntiAffinityProperty.class);
      }

      return success();
    });
    singleVertexCheckerList.add(parallelismWithOtherEPsInSingleVertex);

    final SingleVertexChecker parallelismOfSourceVertex = (v -> {
      final Optional<Integer> parallelism = v.getPropertyValue(ParallelismProperty.class);
      try {
        if (parallelism.isPresent() && v instanceof SourceVertex) {
          final int numOfReadables = ((SourceVertex) v).getReadables(parallelism.get()).size();
          if (parallelism.get() != numOfReadables) {
            return failure(String.format("(Parallelism %d) != (Number of SourceVertex %s Readables %d)",
              parallelism.get(), v.getId(), numOfReadables));
          }
        }
      } catch (Exception e) {
        return failure(e.getMessage());
      }

      return success();
    });
    singleVertexCheckerList.add(parallelismOfSourceVertex);

    final NeighborChecker parallelismWithCommPattern = ((v, inEdges, outEdges) -> {
      // Just look at incoming (edges, as this checker will be applied on every vertex
      for (final IREdge inEdge : inEdges) {
        if (CommunicationPatternProperty.Value.OneToOne
          .equals(inEdge.getPropertyValue(CommunicationPatternProperty.class).get())) {
          if (v.getPropertyValue(ParallelismProperty.class).isPresent()
            && inEdge.getSrc().getPropertyValue(ParallelismProperty.class).isPresent()
            && !inEdge.getSrc().getPropertyValue(ParallelismProperty.class)
            .equals(v.getPropertyValue(ParallelismProperty.class))) {
            return failure("OneToOne edges must have the same parallelism",
              inEdge.getSrc(), ParallelismProperty.class, v, ParallelismProperty.class);
          }
        }
      }

      return success();
    });
    neighborCheckerList.add(parallelismWithCommPattern);

    final NeighborChecker parallelismWithPartitionSet = ((v, inEdges, outEdges) -> {
      final Optional<Integer> parallelism = v.getPropertyValue(ParallelismProperty.class);
      for (final IREdge inEdge : inEdges) {
        final Optional<Integer> keyRangeListSize = inEdge.getPropertyValue(PartitionSetProperty.class)
          .map(keyRangeList -> keyRangeList.size());
        if (parallelism.isPresent() && keyRangeListSize.isPresent() && !parallelism.equals(keyRangeListSize)) {
          return failure("PartitionSet must contain all task offsets required for the dst parallelism",
            v, ParallelismProperty.class, inEdge, PartitionSetProperty.class);
        }
      }

      return success();
    });
    neighborCheckerList.add(parallelismWithPartitionSet);
  }

  void addPartitioningCheckers() {
    final NeighborChecker partitionerAndPartitionSet = ((v, inEdges, outEdges) -> {
      for (final IREdge inEdge : inEdges) {
        final Optional<Pair<PartitionerProperty.Type, Integer>> partitioner =
          inEdge.getPropertyValue(PartitionerProperty.class);
        final Optional<ArrayList<KeyRange>> partitionSet = inEdge.getPropertyValue(PartitionSetProperty.class);
        // Shuffle edge
        if (partitioner.isPresent() && partitionSet.isPresent()) {
          final Set<Integer> flattenedPartitionOffsets = partitionSet.get()
            .stream()
            .flatMap(keyRange -> IntStream.range(
              (int) keyRange.rangeBeginInclusive(), (int) keyRange.rangeEndExclusive()).boxed())
            .collect(Collectors.toSet());
          if (partitioner.get().right() == PartitionerProperty.NUM_EQUAL_TO_DST_PARALLELISM) {
            final Optional<Integer> parallelism = v.getPropertyValue(ParallelismProperty.class);
            if (parallelism.isPresent() &&
              !getZeroToNSet(parallelism.get()).equals(flattenedPartitionOffsets)) {
              return failure("PartitionSet must contain all partition offsets required for dst parallelism",
                v, ParallelismProperty.class, inEdge, PartitionSetProperty.class);
            }
          } else {
            if (!getZeroToNSet(partitioner.get().right()).equals(flattenedPartitionOffsets)) {
              return failure("PartitionSet must contain all partition offsets required for the partitioner",
                inEdge, PartitionerProperty.class, PartitionSetProperty.class);
            }
          }
        }
      }

      return success();
    });
    neighborCheckerList.add(partitionerAndPartitionSet);

    // partition set
  }

  void addShuffleEdgeCheckers() {
    final NeighborChecker shuffleChecker = ((v, inEdges, outEdges) -> {
      for (final IREdge inEdge : inEdges) {
        if (CommunicationPatternProperty.Value.Shuffle
          .equals(inEdge.getPropertyValue(CommunicationPatternProperty.class).get())) {
          // Shuffle edges must have the following properties
          if (!inEdge.getPropertyValue(KeyExtractorProperty.class).isPresent()
            || !inEdge.getPropertyValue(KeyEncoderProperty.class).isPresent()
            || !inEdge.getPropertyValue(KeyDecoderProperty.class).isPresent()) {
            return failure("Shuffle edge does not have a Key-related property: " + inEdge.getId());
          }
        } else {
          // Non-shuffle edges must not have the following properties
          final Optional<Pair<PartitionerProperty.Type, Integer>> partitioner =
            inEdge.getPropertyValue(PartitionerProperty.class);
          if (partitioner.isPresent() && partitioner.get().left().equals(PartitionerProperty.Type.Hash)) {
            return failure("Only shuffle can have the hash partitioner",
              inEdge, CommunicationPatternProperty.class, PartitionerProperty.class);
          }
          if (inEdge.getPropertyValue(PartitionSetProperty.class).isPresent()) {
            return failure("Only shuffle can select partition sets",
              inEdge, CommunicationPatternProperty.class, PartitionSetProperty.class);
          }
        }
      }

      return success();
    });
    neighborCheckerList.add(shuffleChecker);
  }

  /**
   * Parallelism-related checkers.
   */
  void addMessageBarrierCheckers() {
    // Check vertices to optimize
    // Message Ids and same additional-output
    //
  }

  void addStreamVertexCheckers() {

  }

  void addEncodingCompressionCheckers() {
    final NeighborChecker additionalOutputEncoder = ((irVertex, inEdges, outEdges) -> {
      final Map<Optional<String>, List<IREdge>> tagToOutEdges = outEdges.stream().collect(Collectors.groupingBy(
        (outEdge -> outEdge.getPropertyValue(AdditionalOutputTagProperty.class)),
        Collectors.toList()));
      for (final List<IREdge> sameTagOutEdges : tagToOutEdges.values()) {
        if (1 != sameTagOutEdges.stream()
          .map(e -> e.getPropertyValue(EncoderProperty.class).get().getClass()).distinct().count()) {
          return failure("Incompatible encoders in " + sameTagOutEdges.toString());
        }
        if (1 != sameTagOutEdges.stream()
          .map(e -> e.getPropertyValue(DecoderProperty.class).get().getClass()).distinct().count()) {
          return failure("Incompatible decoders in " + sameTagOutEdges.toString());
        }
      }
      return success();
    });
    neighborCheckerList.add(additionalOutputEncoder);

    // TODO: check single-edge encoder/decoder symmetry

    final SingleEdgeChecker compressAndDecompress = (edge -> {
      if (!(edge.getDst() instanceof StreamVertex)) {
        if (edge.getPropertyValue(CompressionProperty.class) != edge.getPropertyValue(DecompressionProperty.class)) {
          return failure("Compression and decompression must be symmetric",
            edge, CompressionProperty.class, DecompressionProperty.class);
        }
      }
      return success();
    });
    singleEdgeCheckerList.add(compressAndDecompress);

    // How to check symmetry?
    // encoder and decoder must be symmetric
    // compressor and decompressor must be asymmetric
    // key encoder and decoder must be symmetric
  }

  /**
   * Applies all of the checkers on the DAG.
   *
   * @param underlyingDAG to check
   * @return the result.
   */
  public CheckerResult doCheck(final DAG<IRVertex, IREdge> underlyingDAG) {
    for (final IRVertex v : underlyingDAG.getTopologicalSort()) {
      // Run per-vertex checkers
      for (final SingleVertexChecker checker : singleVertexCheckerList) {
        final CheckerResult result = checker.check(v);
        if (!result.isPassed()) {
          return result;
        }
      }

      final List<IREdge> inEdges = underlyingDAG.getIncomingEdgesOf(v);
      final List<IREdge> outEdges = underlyingDAG.getOutgoingEdgesOf(v);

      // Run per-edge checkers
      for (final IREdge inEdge : inEdges) {
        for (final SingleEdgeChecker checker : singleEdgeCheckerList) {
          final CheckerResult result = checker.check(inEdge);
          if (!result.isPassed()) {
            return result;
          }
        }
      }

      // Run neighbor edges
      for (final NeighborChecker checker : neighborCheckerList) {
        final CheckerResult result = checker.check(v, inEdges, outEdges);
        if (!result.isPassed()) {
          return result;
        }
      }
    }

    // Run global checkers
    for (final GlobalDAGChecker checker : globalDAGCheckerList) {
      final CheckerResult result = checker.check(underlyingDAG);
      if (!result.isPassed()) {
        return result;
      }
    }

    return success();
  }

  /*

  ////////////////////////////////////////// Execution properties

  public void testVertexEPs() {

    //////////////// User-configurable

    // Uses task indices
    ParallelismProperty;
    ResourceSiteProperty;
    ResourceAntiAffinityProperty;

    // Okay?
    ClonedSchedulingProperty;
    ResourceLocalityProperty;
    ResourcePriorityProperty;
    ResourceSlotProperty;

    //////////////// Configured by Nemo

    // Ordering
    ScheduleGroupProperty;

    // Unknown
    IgnoreSchedulingTempDataReceiverProperty;

    // Dyn Opt
    MessageIdProperty;
  }

  public void testEdgeEPs() {

    //////////////// User-configurable

    // Okay?
    DataFlowProperty;
    DataPersistenceProperty;
    DataStoreProperty;

    // Uses task indices
    PartitionerProperty;
    PartitionSetProperty;

    //////////////// Configured by Nemo

    // Duplicate (should be merged into one)
    DuplicateEdgeGroupProperty;

    // Semantics: Encoder/decoders, compressors, commpattern
    CommunicationPatternProperty;
    DecoderProperty;
    DecompressionProperty;
    EncoderProperty;
    CompressionProperty;
    AdditionalOutputTagProperty;

    // Cache
    CacheIDProperty;

    // Key related (probably should be just one property)
    KeyDecoderProperty;
    KeyEncoderProperty;
    KeyExtractorProperty;
  }

  */

  ///////////////////////////// Successes and Failures

  private final CheckerResult SUCCESS = new CheckerResult(true, "");

  public class CheckerResult {
    private final boolean pass;
    private final String failReason; // empty string if pass = true

    CheckerResult(final boolean pass, final String failReason) {
      this.pass = pass;
      this.failReason = failReason;
    }

    final boolean isPassed() {
      return pass;
    }

    final String getFailReason() {
      return failReason;
    }
  }

  CheckerResult success() {
    return SUCCESS;
  }

  CheckerResult failure(final String failReason) {
    return new CheckerResult(false, failReason);
  }

  CheckerResult failure(final String description,
                        final Object vertexOrEdgeOne, final Class epOne,
                        final Object vertexOrEdgeTwo, final Class epTwo) {
    final CheckerResult failureOne = vertexOrEdgeOne instanceof IRVertex
      ? failure("First", (IRVertex) vertexOrEdgeOne, epOne)
      : failure("First", (IREdge) vertexOrEdgeOne, epOne);
    final CheckerResult failureTwo = vertexOrEdgeTwo instanceof IRVertex
      ? failure("Second", (IRVertex) vertexOrEdgeTwo, epTwo)
      : failure("Second", (IREdge) vertexOrEdgeTwo, epTwo);
    return failure(description + " - ("
      + failureOne.failReason + ") incompatible with (" + failureTwo.failReason + ")");
  }

  CheckerResult failure(final String description,
                        final IRVertex v,
                        final Class... eps) {
    final List<Optional> epsList = Arrays.stream(eps)
      .map(ep -> (Class<VertexExecutionProperty<Serializable>>)ep)
      .map(ep -> v.getPropertyValue(ep))
      .collect(Collectors.toList());
    final boolean isMissingValue = epsList.stream().anyMatch(optional -> !((Optional) optional).isPresent());
    if (isMissingValue) {
      throw new IllegalArgumentException(epsList.toString());
    } else {
      return failure(String.format("%s - [IRVertex %s: %s]", description, v.getId(), epsList.toString()));
    }
  }

  CheckerResult failure(final String description,
                        final IREdge e,
                        final Class... eps) {
    final List<Optional> epsList = Arrays.stream(eps)
      .map(ep -> (Class<EdgeExecutionProperty<Serializable>>)ep)
      .map(ep -> e.getPropertyValue(ep)).collect(Collectors.toList());
    final boolean isMissingValue = epsList.stream().anyMatch(optional -> !((Optional) optional).isPresent());
    if (isMissingValue) {
      throw new IllegalArgumentException(epsList.toString());
    } else {
      return failure(String.format("%s - [IREdge(%s->%s) %s: %s]",
        description, e.getSrc().getId(), e.getDst().getId(), e.getId(), epsList.toString()));
    }
  }
}
