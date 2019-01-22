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
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DecoderProperty;
import org.apache.nemo.common.ir.edge.executionproperty.EncoderProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.system.MessageBarrierVertex;
import org.apache.nemo.common.ir.vertex.system.StreamVertex;
import org.apache.nemo.common.ir.vertex.system.SystemIRVertex;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * An IRDAG object captures a high-level data processing application (e.g., Spark/Beam application).
 * - IRVertex: A data-parallel operation. (e.g., map)
 * - IREdge: A data dependency between two operations. (e.g., shuffle)
 *
 * Largely two types of IRDAG optimization(modification) methods are provided.
 * All of these methods preserve application semantics.
 * - Annotation: setProperty(), getPropertyValue() on each IRVertex/IREdge
 * - Reshaping: insert(), delete() on the IRDAG
 */
public class IRDAG {
  private DAG<IRVertex, IREdge> dag; // internal DAG, can be updated by reshaping methods.

  public IRDAG(final DAG<IRVertex, IREdge> dag) {
    this.dag = dag;
  }

  ////////////////////////////////////////////////// Read-only traversal methods for annotations.

  public void topologicalDo(final Consumer<IRVertex> function) {
    dag.topologicalDo(function);
  }

  public List<IRVertex> getVertices() {
    return dag.getVertices();
  }

  public List<IREdge> getIncomingEdgesOf(final String vertexId) {
    return dag.getIncomingEdgesOf(vertexId);
  }

  public List<IREdge> getIncomingEdgesOf(final IRVertex v) {
    return dag.getIncomingEdgesOf(v);
  }

  ////////////////////////////////////////////////// Reshaping methods.

  /**
   * Before: src > edgeToStreamize > dst
   * After: src > edgeToStreamizeWithNewDestination > streamVertex > oneToOneEdge > dst
   * (replaces the "Before" relationships)
   *
   * @param streamVertex to insert.
   * @param edgeToStreamize for inserting.
   */
  public void insert(final StreamVertex streamVertex, final IREdge edgeToStreamize) {
    // Create a completely new DAG with the vertex inserted.
    final DAGBuilder builder = new DAGBuilder();

    // Insert the vertex.
    builder.addVertex(streamVertex);

    // Build the new DAG to reflect the new topology.
    dag.topologicalDo(v -> {
      // None of the existing vertices are deleted.
      builder.addVertex(v);

      if (edgeToStreamize.getDst().equals(v)
        && dag.getIncomingEdgesOf(v).stream().anyMatch(e -> e.equals(edgeToStreamize))) {
        // Edge to the streamVertex.
        final IREdge edgeToStreamizeWithNewDestination = new IREdge(
          edgeToStreamize.getPropertyValue(CommunicationPatternProperty.class).get(),
          edgeToStreamize.getSrc(),
          streamVertex);
        edgeToStreamize.copyExecutionPropertiesTo(edgeToStreamizeWithNewDestination);

        // Edge from the streamVertex.
        final IREdge oneToOneEdge = new IREdge(CommunicationPatternProperty.Value.OneToOne, streamVertex, v);
        oneToOneEdge.setProperty(EncoderProperty.of(edgeToStreamize.getPropertyValue(EncoderProperty.class).get()));
        oneToOneEdge.setProperty(DecoderProperty.of(edgeToStreamize.getPropertyValue(DecoderProperty.class).get()));

        // Track the new edges.
        builder.connectVertices(edgeToStreamize);
        builder.connectVertices(oneToOneEdge);
      } else {
        // Not the destination of the edgeToStreamize, so simply connect vertices as before.
        dag.getIncomingEdgesOf(v).forEach(builder::connectVertices);
      }
    });

    dag = builder.build(); // update the DAG.
  }

  /**
   * Before: src > edgeToGetStatisticsOf > dst
   * After: src > oneToOneEdge(a clone of edgeToGetStatisticsOf) > messageBarrierVertex
   * (the "Before" relationships are unmodified)
   *
   * @param messageBarrierVertex
   * @param edgeToGetStatisticsOf
   */
  public void insert(final MessageBarrierVertex messageBarrierVertex, final IREdge edgeToGetStatisticsOf) {
  }

  /**
   * @param systemIRVertexToDelete to delete.
   */
  public void delete(final SystemIRVertex systemIRVertexToDelete) {
    // TODO: recursively delete backwards
  }

  ////////////////////////////////////////////////// "Un-safe" direct reshaping (semantic-preserving not guaranteed)

  public void unSafeDirectReshaping(final Function<DAG<IRVertex, IREdge>, DAG<IRVertex, IREdge>> unsafeReshaping) {
    this.dag = unsafeReshaping.apply(dag);
  }
}
