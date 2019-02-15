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

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.executionproperty.ExecutionProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.*;
import org.apache.nemo.common.ir.vertex.utility.MessageBarrierVertex;
import org.apache.nemo.common.ir.vertex.utility.StreamVertex;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Checker range:
 * single vertex, single edge, neighbors, all
 *
 * Checker properties:
 * vertex property, edge property, combination with utility vertices and communication patterns
 *
 * Checker exception
 * A generic layer? (for analysis/debugging)
 */
public class IRDAGChecker {

  ///////////////////////////// Checkers to use

  private final List<SingleVertexChecker> singleVertexCheckerList;
  private final List<SingleEdgeChecker> singleEdgeCheckerList;
  private final List<NeighborChecker> neighborCheckerList;
  private final List<GlobalDAGChecker> globalDAGCheckerList;

  private interface SingleVertexChecker {
    CheckerResult check(final IRVertex irVertex);
  }

  private interface SingleEdgeChecker {
    CheckerResult check(final IREdge irEdge);
  }

  private interface NeighborChecker {
    CheckerResult check(final IRVertex irVertex,
                        final List<IREdge> inEdges,
                        final List<IREdge> outEdges);
  }

  public interface GlobalDAGChecker {
    CheckerResult check(final IRDAG irdag);
  }

  ///////////////////////////// Successes and Failures

  public final static CheckerResult SUCCESS = new CheckerResult();

  private class CheckerResult {
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
    return new CheckerResult(true, "");
  }

  CheckerResult failure(final String failReason) {
    return new CheckerResult(false, failReason);
  }

  CheckerResult failure(final IRVertex v, final Class... eps) {
    final List<Class> epsList = Arrays.asList(eps);
    final boolean isMissingValue = epsList.stream()
      .map(ep -> v.getPropertyValue(ep))
      .anyMatch(optional -> !optional.isPresent());

    if (isMissingValue) {
      throw new IllegalArgumentException(epsList.toString());
    } else {
      return failure(String.format("IRVertex %s incompatible properties: %s", v.getId(), epsList.toString()));
    }
  }

  CheckerResult failure(final IREdge e, final Class... eps) {
    final List<Class> epsList = Arrays.asList(eps);
    final boolean isMissingValue = epsList.stream()
      .map(ep -> e.getPropertyValue(ep))
      .anyMatch(optional -> !optional.isPresent());

    if (isMissingValue) {
      throw new IllegalArgumentException(epsList.toString());
    } else {
      return failure(String.format("IREdge %s incompatible properties: %s", e.getId(), epsList.toString()));
    }
  }

  /*
  CheckerResult failure(final IRVertex v, Class... eps) {
    final List<Class> epsList = Arrays.asList(eps);
    final boolean isAllSame = epsList.stream()
      .map(ep -> v.getPropertyValue(ep))
      .distinct()
      .limit(2)
      .count() <= 1;
      .anyMatch(optional -> !optional.isPresent());

    if (missingValue) {
      return failure();
    } else {
      epsList.stream()
        .map(ep -> v.getPropertyValue(ep))
    }
  }
  */

  ///////////////////////////// Set up

  public void setUp() {

    /////////// Parallelism-related checkers

    final SingleVertexChecker parallelismInSingleVertex = (v -> {
      final Optional<Integer> parallelism = v.getPropertyValue(ParallelismProperty.class);
      final Optional<Integer> resourceSiteSize
        = v.getPropertyValue(ResourceSiteProperty.class).map(rs -> rs.size());
      final Optional<Integer> antiAffinitySize
        = v.getPropertyValue(ResourceAntiAffinityProperty.class).map(raa -> raa.size());

      if (!parallelism.isPresent()) {
        return success(); // No need to check, if the parallelism is not set yet
      } else if (!parallelism.equals(resourceSiteSize)) {
        return failure(v, ParallelismProperty.class, ResourceSiteProperty.class);
      } else if (!parallelism.equals(antiAffinitySize)) {
        return failure(v, ParallelismProperty.class, ResourceAntiAffinityProperty.class);
      } else {
        return success();
      }
    });

    final SingleVertexChecker parallelismInSourceVertex = (v -> {
      final Optional<Integer> parallelism = v.getPropertyValue(ParallelismProperty.class);

      try {
        if (parallelism.isPresent() && v instanceof SourceVertex) {
          if (parallelism.get() != ((SourceVertex) v).getReadables(parallelism.get()).size()) {
            return failure();
          }
        }
      } catch (Exception e) {
        return failure(e.getMessage());
      }

      return success();
    });

    final NeighborChecker parallelismWithCommPattern = (v, inEdges, outEdges) -> {
      // Just look at incoming edges, as this checker will be applied on every vertex
      for (final IREdge inEdge : inEdges) {
        if (CommunicationPatternProperty.Value.OneToOne
          .equals(inEdge.getPropertyValue(CommunicationPatternProperty.class).get())) {
          if (inEdge.getSrc().getPropertyValue(ParallelismProperty.class)
            .equals(v.getPropertyValue(ParallelismProperty.class))) {
            return failure();
          }
        }
      }

      return success();
    }

    final NeighborChecker parallelismWithPartitioners = (v, inEdges, outEdges) -> {
      final Optional<Integer> parallelism = v.getPropertyValue(ParallelismProperty.class);
      if (!parallelism.isPresent()) {
        return success(); // No need to check, if the parallelism is not set yet
      }

      for (final IREdge inEdge : inEdges) {
        final Optional<List<Integer>> partitionSetFlattened = inEdge.getPropertyValue(PartitionSetProperty.class)
          .flatMap(ps -> ps.stream().flatMap(keyRange ->
            IntStream.range((int) keyRange.rangeBeginInclusive(), (int) keyRange.rangeEndExclusive()).boxed()));

      }

      for (final IREdge outEdge : outEdges) {
        final Optional<Integer> partitionerNum =
          outEdge.getPropertyValue(PartitionerProperty.class).map(pp -> pp.right());
        if (!parallelism.equals(partitionerNum)) {
          return failure()
        }
      }

      return success();
    }

    /////////// Other checkers


    // parallelism and must match

    // encoder and decoder must match

    // compressor

    // key encoder and decoder

    // same-additional output tag should have same encoder/decoders

  }

  ///////////////////////////// Set up

  private CheckerResult doCheck(final DAG<IRVertex, IREdge> underlyingDAG) {
    underlyingDAG.topologicalDo(v -> {
      // Run per-vertex checkers
      singleVertexCheckerList.forEach(checker -> {
        final CheckerResult result = checker.check(v);
        if (result) {
          return result;
        }
      });

      final List<IREdge> inEdges = underlyingDAG.getIncomingEdgesOf(v);
      final List<IREdge> outEdges = underlyingDAG.getOutgoingEdgesOf(v);

      // Run per-edge checkers
      inEdges.forEach(e -> {
        singleEdgeCheckerList.forEach(checker -> {
          final CheckerResult result = checker.check(e);
          if (result) {
            return result;
          }
        });
      });

      // Run neighbor edges
      neighborCheckerList.forEach(checker -> {
        final CheckerResult result = checker.check(v, inEdges, outEdges);
        if (result) {
          return result;
        }
      });
    });

    // Run global checkers
    globalDAGCheckerList.forEach(checker -> {
      final CheckerResult result = checker.check(underlyingDAG);
      if (result) {
        return result;
      }
    });

    // All passed
    return success();
  }

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
}
