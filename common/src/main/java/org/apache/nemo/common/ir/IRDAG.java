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

import org.apache.nemo.common.KeyExtractor;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.system.MessageAggregationVertex;
import org.apache.nemo.common.ir.vertex.system.MessageBarrierVertex;
import org.apache.nemo.common.ir.vertex.system.StreamVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

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
public final class IRDAG {
  private static final Logger LOG = LoggerFactory.getLogger(IRDAG.class.getName());

  private DAG<IRVertex, IREdge> dag; // internal DAG, can be updated by reshaping methods.

  public IRDAG(final DAG<IRVertex, IREdge> dag) {
    this.dag = dag;
  }

  ////////////////////////////////////////////////// Read-only traversal methods.

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

  public List<IREdge> getOutgoingEdgesOf(final IRVertex v) {
    return dag.getOutgoingEdgesOf(v);
  }

  public List<IREdge> getOutgoingEdgesOf(final String vertexId) {
    return dag.getOutgoingEdgesOf(vertexId);
  }

  public void storeJSON(final String directory, final String name, final String description) {
    dag.storeJSON(directory, name, description);
  }

  public List<IRVertex> getTopologicalSort() {
    return dag.getTopologicalSort();
  }

  public List<IRVertex> getDescendants(final String vertexId) {
    return dag.getDescendants(vertexId);
  }

  public IRVertex getVertexById(final String id) {
    return dag.getVertexById(id);
  }

  public List<IRVertex> filterVertices(final Predicate<IRVertex> condition) {
    return dag.filterVertices(condition);
  }

  public List<IRVertex> getParents(final String vertexId) {
    return dag.getParents(vertexId);
  }

  public List<IRVertex> getChildren(final String vertexId) {
    return dag.getChildren(vertexId);
  }

  public DAG<IRVertex, IREdge> getUnderlyingDAG() {
    return dag;
  }

  ////////////////////////////////////////////////// Reshaping methods.

  /**
   * Inserts a new vertex that streams data.
   *
   * Before: src - edgeToStreamize - dst
   * After: src - edgeToStreamizeWithNewDestination - streamVertex - oneToOneEdge - dst
   * (replaces the "Before" relationships)
   *
   * @param streamVertex to insert.
   * @param edgeToStreamize to modify.
   */
  public void insert(final StreamVertex streamVertex, final IREdge edgeToStreamize) {
    // Create a completely new DAG with the vertex inserted.
    final DAGBuilder builder = new DAGBuilder();

    // Insert the vertex.
    builder.addVertex(streamVertex);

    // Build the new DAG to reflect the new topology.
    dag.topologicalDo(v -> {
      builder.addVertex(v); // None of the existing vertices are deleted.

      for (final IREdge edge : dag.getIncomingEdgesOf(v)) {
        if (edge.equals(edgeToStreamize)) {
          // MATCH!

          // Edge to the streamVertex
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
          builder.connectVertices(edgeToStreamizeWithNewDestination);
          builder.connectVertices(oneToOneEdge);
        } else {
          // NO MATCH, so simply connect vertices as before.
          builder.connectVertices(edge);
        }
      }
    });

    dag = builder.build(); // update the DAG.
  }

  /**
   * Inserts a new vertex that analyzes intermediate data, and triggers a dynamic optimization.
   *
   * Before: src - edgeToGetStatisticsOf - dst
   * After: src - oneToOneEdge(a clone of edgeToGetStatisticsOf) - messageBarrierVertex -
   *        shuffleEdge - messageAggregationVertex - broadcastEdge - dst
   * (the "Before" relationships are unmodified)
   *
   * @param messageBarrierVertex to insert.
   * @param messageAggregationVertex to insert.
   * @param edgeToGetStatisticsOf to clone and examine.
   */
  public void insert(final MessageBarrierVertex messageBarrierVertex,
                     final MessageAggregationVertex messageAggregationVertex,
                     final EncoderProperty mbvOutputEncoder,
                     final DecoderProperty mbvOutputDecoder,
                     final IREdge edgeToGetStatisticsOf) {
    // Create a completely new DAG with the vertex inserted.
    final DAGBuilder builder = new DAGBuilder();

    // Insert the vertex.
    builder.addVertex(messageBarrierVertex);

    dag.topologicalDo(v -> {
      // None of the existing vertices are deleted.
      builder.addVertex(v);

      for (final IREdge edge : dag.getIncomingEdgesOf(v)) {
        if (edge.equals(edgeToGetStatisticsOf)) {
          // MATCH!
          builder.addVertex(messageBarrierVertex);
          builder.addVertex(messageAggregationVertex);

          // Clone the edgeToGetStatisticsOf
          final IREdge clone = new IREdge(
            CommunicationPatternProperty.Value.OneToOne, edge.getSrc(), messageBarrierVertex);
          clone.setProperty(EncoderProperty.of(edge.getPropertyValue(EncoderProperty.class).get()));
          clone.setProperty(DecoderProperty.of(edge.getPropertyValue(DecoderProperty.class).get()));
          edge.getPropertyValue(AdditionalOutputTagProperty.class).ifPresent(tag -> {
            clone.setProperty(AdditionalOutputTagProperty.of(tag));
          });
          builder.connectVertices(clone);

          // messageBarrierVertex to the messageAggregationVertex
          final IREdge edgeToABV = edgeBetweenMessageVertices(
            messageBarrierVertex, messageAggregationVertex, mbvOutputEncoder, mbvOutputDecoder);
          builder.connectVertices(edgeToABV);

          // Connection vertex
          // Add a control dependency (no output)
          final IREdge emptyEdge =
            new IREdge(CommunicationPatternProperty.Value.BroadCast, messageAggregationVertex, v);
          builder.connectVertices(emptyEdge);

          // The original edge
          // We then insert the vertex with MessageBarrierTransform and vertex with MessageAggregateTransform
          // between the vertex and incoming vertices.
          final IREdge edgeToOriginalDst =
            new IREdge(edge.getPropertyValue(CommunicationPatternProperty.class).get(), edge.getSrc(), v);
          edge.copyExecutionPropertiesTo(edgeToOriginalDst);
          edgeToOriginalDst.setPropertyPermanently(
            MetricCollectionProperty.of(MetricCollectionProperty.Value.DataSkewRuntimePass));
          builder.connectVertices(edgeToOriginalDst);
        } else {
          // NO MATCH, so simply connect vertices as before.
          builder.connectVertices(edge);
        }
      }
    });

    dag = builder.build(); // update the DAG.
  }

  ////////////////////////////////////////////////// "Un-safe" direct reshaping (semantic-preserving is not guaranteed).

  public void unSafeDirectReshaping(final Function<DAG<IRVertex, IREdge>, DAG<IRVertex, IREdge>> unsafeReshaping) {
    this.dag = unsafeReshaping.apply(dag);
  }

  ////////////////////////////////////////////////// Private helper methods.

  /**
   * @param mbv the vertex with MessageBarrierTransform.
   * @param mav the vertex with MessageAggregateTransform.
   * @return the generated egde from {@code mcv} to {@code abv}.
   */
  private IREdge edgeBetweenMessageVertices(final MessageBarrierVertex mbv,
                                            final MessageAggregationVertex mav,
                                            final EncoderProperty encoder,
                                            final DecoderProperty decoder) {
    final IREdge newEdge = new IREdge(CommunicationPatternProperty.Value.Shuffle, mbv, mav);
    newEdge.setProperty(DataStoreProperty.of(DataStoreProperty.Value.LocalFileStore));
    newEdge.setProperty(DataPersistenceProperty.of(DataPersistenceProperty.Value.Keep));
    newEdge.setProperty(DataFlowProperty.of(DataFlowProperty.Value.Push));
    final KeyExtractor pairKeyExtractor = (element) -> {
      if (element instanceof Pair) {
        return ((Pair) element).left();
      } else {
        throw new IllegalStateException(element.toString());
      }
    };
    newEdge.setProperty(KeyExtractorProperty.of(pairKeyExtractor));
    newEdge.setPropertyPermanently(encoder);
    newEdge.setPropertyPermanently(decoder);
    return newEdge;
  }
}
