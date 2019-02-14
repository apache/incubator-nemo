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

import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.*;

import java.util.List;

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
  public void setUp() {
    // IRDAG
    // IRDAG => builds using DAGBuilder
    // IRDAG => check semantics using IRDAGChecker
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
