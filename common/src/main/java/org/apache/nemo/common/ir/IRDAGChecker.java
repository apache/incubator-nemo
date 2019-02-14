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
import org.apache.nemo.common.ir.vertex.executionproperty.*;
import org.apache.nemo.common.ir.vertex.utility.MessageBarrierVertex;
import org.apache.nemo.common.ir.vertex.utility.StreamVertex;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

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

  public interface SingleVertexChecker {
    CheckerResult check(final IRVertex irVertex);
  }

  public interface SingleEdgeChecker {
    CheckerResult check(final IREdge irEdge);
  }

  public interface NeighborChecker {
    CheckerResult check(final IRVertex irVertex,
                        final List<IREdge> inEdges,
                        final List<IREdge> outEdges);
  }

  public interface GlobalDAGChecker {
    CheckerResult check(final IRDAG irdag);
  }

  ///////////////////////////// Successes and Failures

  private class CheckerResult {
    final boolean pass;
    final String failReason;
  }


  CheckerResult success() {
  }

  CheckerResult failure(final Object vertexOrEdge, Class... eps) {
    // Two exception reasons
    //
    // (1) ExecutionProperty compatibility
    // (2) ExecutionProperty - utility vertex compatibility
    if (vertexOrEdge instanceof IRVertex) {
      final String x = "IRVertex " + getId() + " incompatible"

    } else if (vertexOrEdge instanceof IREdge) {

    } else {

    }

    super("Incompatible execution properties (IRVertex/IREdge id, execution property): "
      + Arrays.asList(idEPPairs).toString());
  }

  CheckerResult failure(final Object vertexOrEdge, Class... eps) {
  }

  ///////////////////////////// Set up

  public void setUp() {
    // IRDAG
    // IRDAG => builds using DAGBuilder
    // IRDAG => check semantics using IRDAGChecker

    final SingleEdgeChecker singleEdgeChecker = (irEdge) -> irEdge.getSrc();

    // Single vertex parallelism
    final SingleVertexChecker parallelismInSingleVertex = (v -> {
      final int parallelism = v.getPropertyValue(ParallelismProperty.class).get();

      v.getPropertyValue(ResourceSiteProperty.class).ifPresent(rs -> {
        if (rs.size() != parallelism) {
          return failure(v, ParallelismProperty.class, ResourceSiteProperty.class);
        }
      });

      v.getPropertyValue(ResourceAntiAffinityProperty.class).ifPresent(raa -> {
        if (raa.size() != parallelism) {
          return failure(v, ParallelismProperty.class, ResourceSiteProperty.class);
        }
      });

      return success();
    });

    // Something something



    // Something something



    // Something something
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

  private boolean checkSingleEdge(final IREdge edge) {
    // parallelism and must match

    // encoder and decoder must match

    // compressor

    // key encoder and decoder
  }

  private boolean checkSingleVertex(final IRVertex vertex) {
    final int parallelism = vertex.getPropertyValue(ParallelismProperty.class).get();
    vertex.getPropertyValue(ResourceSiteProperty.class).ifPresent(rs -> {
      rs.size() == parallelism;
    });
    vertex.getPropertyValue(ResourceAntiAffinityProperty.class).ifPresent(raa -> {
      raa.size() == parallelism;
    });
  }

  private boolean checkNeighbors(final IRVertex vertex,
                                 final List<IREdge> inEdges,
                                 final List<IREdge> outEdges) {
    final int parallelism = vertex.getPropertyValue(ParallelismProperty.class).get();

    inEdges.forEach(inEdge -> {
      inEdge.getPropertyValue(PartitionerProperty.class).ifPresent(pp -> {
        pp.right() == parallelism;
      });

      inEdge.getPropertyValue(PartitionSetProperty.class).ifPresent(ps -> {
        ps.size() == parallelism;
      });
    });
  }

  private boolean checkNeighborsTwo(final IRVertex vertex,
                                    final List<IREdge> inEdges,
                                    final List<IREdge> outEdges) {
    // same-additional output tag should have same encoder/decoders

    //
  }

  private boolean checkStreamVertex(final StreamVertex streamVertex, final IREdge inEdge, final IREdge outEdge) {
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
