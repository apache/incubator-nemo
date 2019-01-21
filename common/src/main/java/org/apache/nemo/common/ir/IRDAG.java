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

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DecoderProperty;
import org.apache.nemo.common.ir.edge.executionproperty.EncoderProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.LoopVertex;
import org.apache.nemo.common.ir.vertex.system.MessageBarrierVertex;
import org.apache.nemo.common.ir.vertex.system.StreamVertex;
import org.apache.nemo.common.ir.vertex.system.SystemIRVertex;

import java.util.Map;
import java.util.Set;

/**
 * An IRDAG object captures a high-level data processing application (e.g., Spark/Beam application).
 * - IRVertex: A data-parallel operation. (e.g., map)
 * - IREdge: A data dependency between two operations. (e.g., shuffle)
 *
 * Largely two types of IRDAG optimization methods are provided.
 * All of these methods preserve application semantics.
 * - Reshaping: insert(), delete() on an IRDAG
 * - Annotation: setProperty(), getPropertyValue() on IRVertex/IREdge
 */
public class IRDAG extends DAG<IRVertex, IREdge> {
  public IRDAG(final Set<IRVertex> vertices,
               final Map<IRVertex, Set<IREdge>> incomingEdges,
               final Map<IRVertex, Set<IREdge>> outgoingEdges,
               final Map<IRVertex, LoopVertex> assignedLoopVertexMap,
               final Map<IRVertex, Integer> loopStackDepthMap) {
    super(vertices, incomingEdges, outgoingEdges, assignedLoopVertexMap, loopStackDepthMap);
  }

  /**
   * Before: src > edgeToStreamize > dst
   * After: src > edgeToStreamize > streamVertex > oneToOneEdge > dst
   * (replaces the "Before" relationships)
   *
   * @param streamVertex to insert.
   * @param edgeToStreamize for inserting.
   */
  void insert(final StreamVertex streamVertex, final IREdge edgeToStreamize) {
    builder.addVertex(streamVertex);
    edge.copyExecutionPropertiesTo(edgeToStreamize);
    final IREdge newEdgeFromMerger = new IREdge(CommunicationPatternProperty.Value.OneToOne, streamVertex, v);
    newEdgeFromMerger.setProperty(EncoderProperty.of(edge.getPropertyValue(EncoderProperty.class).get()));
    newEdgeFromMerger.setProperty(DecoderProperty.of(edge.getPropertyValue(DecoderProperty.class).get()));
    builder.connectVertices(edgeToStreamize);
    builder.connectVertices(newEdgeFromMerger);
  }

  /**
   * Before: src > edgeToGetStatisticsOf > dst
   * After: src > oneToOneEdge(clone of edgeToGetStatisticsOf) > messageBarrierVertex
   * (leaves the "Before" relationships unmodified)
   *
   * @param messageBarrierVertex
   * @param edgeToGetStatisticsOf
   */
  void insert(final MessageBarrierVertex messageBarrierVertex, final IREdge edgeToGetStatisticsOf) {
  }


  void delete(final SystemIRVertex systemIRVertexToDelete) {
  }
}
