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

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.nemo.common.KeyExtractor;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.dag.DAGQueryInterface;
import org.apache.nemo.common.exception.IllegalEdgeOperationException;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.LoopVertex;
import org.apache.nemo.common.ir.vertex.system.MessageAggregatorVertex;
import org.apache.nemo.common.ir.vertex.system.MessageBarrierVertex;
import org.apache.nemo.common.ir.vertex.system.StreamVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

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
@NotThreadSafe
public final class IRDAG implements DAGQueryInterface<IRVertex, IREdge> {
  private static final Logger LOG = LoggerFactory.getLogger(IRDAG.class.getName());

  private final AtomicInteger metricCollectionId;

  private DAG<IRVertex, IREdge> lastSnapshotDAG; // the DAG that was saved most recently.
  private DAG<IRVertex, IREdge> updatingDAG; // the DAG that is being updated.

  /**
   * @param originalUserApplicationDAG the initial DAG.
   */
  public IRDAG(final DAG<IRVertex, IREdge> originalUserApplicationDAG) {
    this.updatingDAG = originalUserApplicationDAG;
    this.lastSnapshotDAG = originalUserApplicationDAG;
    this.metricCollectionId = new AtomicInteger(0);
  }

  //////////////////////////////////////////////////

  /**
   * Used internally by Nemo to advance the DAG snapshot after applying each pass.
   * @param checker that compares the lastSnapshotDAG and the updatingDAG
   *                to determine if the snapshot can be set the current updatingDAG.
   * @return true if the checker passes, false otherwise.
   */
  public boolean advanceDAGSnapshot(final BiFunction<IRDAG, IRDAG, Boolean> checker) {
    final boolean canAdvance = checker.apply(new IRDAG(lastSnapshotDAG), new IRDAG(updatingDAG));
    if (canAdvance) {
      lastSnapshotDAG = updatingDAG;
    }
    return canAdvance;
  }

  ////////////////////////////////////////////////// Methods for reshaping the DAG topology.

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
    updatingDAG.topologicalDo(v -> {
      builder.addVertex(v); // None of the existing vertices are deleted.

      for (final IREdge edge : updatingDAG.getIncomingEdgesOf(v)) {
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

    updatingDAG = builder.build(); // update the DAG.
  }

  /**
   * Inserts a new vertex that analyzes intermediate data, and triggers a dynamic optimization.
   *
   * For each edge in edgesToGetStatisticsOf...
   *
   * Before: src - edge - dst
   * After: src - oneToOneEdge(a clone of edge) - messageBarrierVertex -
   *        shuffleEdge - messageAggregatorVertex - broadcastEdge - dst
   * (the "Before" relationships are unmodified)
   *
   * @param messageBarrierVertex to insert.
   * @param messageAggregatorVertex to insert.
   * @param mbvOutputEncoder to use.
   * @param mbvOutputDecoder to use.
   * @param edgesToGetStatisticsOf to examine.
   */
  public void insert(final MessageBarrierVertex messageBarrierVertex,
                     final MessageAggregatorVertex messageAggregatorVertex,
                     final EncoderProperty mbvOutputEncoder,
                     final DecoderProperty mbvOutputDecoder,
                     final Set<IREdge> edgesToGetStatisticsOf) {
    if (edgesToGetStatisticsOf.stream().map(edge -> edge.getDst().getId()).collect(Collectors.toSet()).size() != 1) {
      throw new IllegalArgumentException("Not destined to the same vertex: " + edgesToGetStatisticsOf.toString());
    }
    final IRVertex dst = edgesToGetStatisticsOf.iterator().next().getDst();

    // Create a completely new DAG with the vertex inserted.
    final DAGBuilder builder = new DAGBuilder();

    // Current metric collection id.
    final int currentMetricCollectionId = metricCollectionId.incrementAndGet();

    // First, add all the vertices.
    updatingDAG.topologicalDo(v -> builder.addVertex(v));

    // Add a control dependency (no output) from the messageAggregatorVertex to the destination.
    builder.addVertex(messageAggregatorVertex);
    final IREdge noDataEdge = new IREdge(CommunicationPatternProperty.Value.BroadCast, messageAggregatorVertex, dst);
    builder.connectVertices(noDataEdge);

    // Add the edges and the messageBarrierVertex.
    updatingDAG.topologicalDo(v -> {
      for (final IREdge edge : updatingDAG.getIncomingEdgesOf(v)) {
        if (edgesToGetStatisticsOf.contains(edge)) {
          // MATCH!
          final MessageBarrierVertex mbv = new MessageBarrierVertex<>(messageBarrierVertex.getMessageFunction());
          builder.addVertex(mbv);

          // Clone the edgeToGetStatisticsOf
          final IREdge clone = new IREdge(CommunicationPatternProperty.Value.OneToOne, edge.getSrc(), mbv);
          clone.setProperty(EncoderProperty.of(edge.getPropertyValue(EncoderProperty.class).get()));
          clone.setProperty(DecoderProperty.of(edge.getPropertyValue(DecoderProperty.class).get()));
          edge.getPropertyValue(AdditionalOutputTagProperty.class).ifPresent(tag -> {
            clone.setProperty(AdditionalOutputTagProperty.of(tag));
          });
          builder.connectVertices(clone);

          // messageBarrierVertex to the messageAggregatorVertex
          final IREdge edgeToABV = edgeBetweenMessageVertices(mbv,
            messageAggregatorVertex, mbvOutputEncoder, mbvOutputDecoder, currentMetricCollectionId);
          builder.connectVertices(edgeToABV);

          // The original edge
          // We then insert the vertex with MessageBarrierTransform and vertex with MessageAggregatorTransform
          // between the vertex and incoming vertices.
          final IREdge edgeToOriginalDst =
            new IREdge(edge.getPropertyValue(CommunicationPatternProperty.class).get(), edge.getSrc(), v);
          edge.copyExecutionPropertiesTo(edgeToOriginalDst);
          edgeToOriginalDst.setPropertyPermanently(MessageIdProperty.of(currentMetricCollectionId));
          builder.connectVertices(edgeToOriginalDst);
        } else {
          // NO MATCH, so simply connect vertices as before.
          builder.connectVertices(edge);
        }
      }
    });

    updatingDAG = builder.build(); // update the DAG.
  }

  /**
   * Reshape unsafely, without guarantees on preserving application semantics.
   * TODO #330: Refactor Unsafe Reshaping Passes
   * @param unsafeReshapingFunction takes as input the underlying DAG, and outputs a reshaped DAG.
   */
  public void reshapeUnsafely(final Function<DAG<IRVertex, IREdge>, DAG<IRVertex, IREdge>> unsafeReshapingFunction) {
    updatingDAG = unsafeReshapingFunction.apply(updatingDAG);
  }

  ////////////////////////////////////////////////// Private helper methods.

  /**
   * @param mbv src.
   * @param mav dst.
   * @param encoder src-dst encoder.
   * @param decoder src-dst decoder.
   * @param currentMetricCollectionId of the edge.
   * @return the edge.
   */
  private IREdge edgeBetweenMessageVertices(final MessageBarrierVertex mbv,
                                            final MessageAggregatorVertex mav,
                                            final EncoderProperty encoder,
                                            final DecoderProperty decoder,
                                            final int currentMetricCollectionId) {
    final IREdge newEdge = new IREdge(CommunicationPatternProperty.Value.Shuffle, mbv, mav);
    newEdge.setProperty(DataStoreProperty.of(DataStoreProperty.Value.LocalFileStore));
    newEdge.setProperty(DataPersistenceProperty.of(DataPersistenceProperty.Value.Keep));
    newEdge.setProperty(DataFlowProperty.of(DataFlowProperty.Value.Push));
    newEdge.setPropertyPermanently(MessageIdProperty.of(currentMetricCollectionId));
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

  ////////////////////////////////////////////////// DAGQueryInterface methods - forward calls to the underlying DAG.

  @Override
  public void topologicalDo(final Consumer<IRVertex> function) {
    updatingDAG.topologicalDo(function);
  }

  @Override
  public void dfsTraverse(final Consumer<IRVertex> function, final TraversalOrder traversalOrder) {
    updatingDAG.dfsTraverse(function, traversalOrder);
  }

  @Override
  public void dfsDo(final IRVertex vertex,
                    final Consumer<IRVertex> vertexConsumer,
                    final TraversalOrder traversalOrder,
                    final Set<IRVertex> visited) {
    updatingDAG.dfsDo(vertex, vertexConsumer, traversalOrder, visited);
  }

  @Override
  public Boolean pathExistsBetween(final IRVertex v1, final IRVertex v2) {
    return updatingDAG.pathExistsBetween(v1, v2);
  }

  @Override
  public Boolean isCompositeVertex(final IRVertex irVertex) {
    return updatingDAG.isCompositeVertex(irVertex);
  }

  @Override
  public Integer getLoopStackDepthOf(final IRVertex irVertex) {
    return updatingDAG.getLoopStackDepthOf(irVertex);
  }

  @Override
  public LoopVertex getAssignedLoopVertexOf(final IRVertex irVertex) {
    return updatingDAG.getAssignedLoopVertexOf(irVertex);
  }

  @Override
  public ObjectNode asJsonNode() {
    return updatingDAG.asJsonNode();
  }

  @Override
  public void storeJSON(final String directory, final String name, final String description) {
    updatingDAG.storeJSON(directory, name, description);
  }

  @Override
  public IRVertex getVertexById(final String id) {
    return updatingDAG.getVertexById(id);
  }

  @Override
  public List<IRVertex> getVertices() {
    return updatingDAG.getVertices();
  }

  @Override
  public List<IRVertex> getRootVertices() {
    return updatingDAG.getRootVertices();
  }

  @Override
  public List<IREdge> getIncomingEdgesOf(final IRVertex v) {
    return updatingDAG.getIncomingEdgesOf(v);
  }

  @Override
  public List<IREdge> getIncomingEdgesOf(final String vertexId) {
    return updatingDAG.getIncomingEdgesOf(vertexId);
  }

  @Override
  public List<IREdge> getOutgoingEdgesOf(final IRVertex v) {
    return updatingDAG.getOutgoingEdgesOf(v);
  }

  @Override
  public List<IREdge> getOutgoingEdgesOf(final String vertexId) {
    return updatingDAG.getOutgoingEdgesOf(vertexId);
  }

  @Override
  public List<IRVertex> getParents(final String vertexId) {
    return updatingDAG.getParents(vertexId);
  }

  @Override
  public List<IRVertex> getChildren(final String vertexId) {
    return updatingDAG.getChildren(vertexId);
  }

  @Override
  public IREdge getEdgeBetween(final String srcVertexId,
                               final String dstVertexId) throws IllegalEdgeOperationException {
    return updatingDAG.getEdgeBetween(srcVertexId, dstVertexId);
  }

  @Override
  public List<IRVertex> getTopologicalSort() {
    return updatingDAG.getTopologicalSort();
  }

  @Override
  public List<IRVertex> getAncestors(final String vertexId) {
    return updatingDAG.getAncestors(vertexId);
  }

  @Override
  public List<IRVertex> getDescendants(final String vertexId) {
    return updatingDAG.getDescendants(vertexId);
  }

  @Override
  public List<IRVertex> filterVertices(final Predicate<IRVertex> condition) {
    return updatingDAG.filterVertices(condition);
  }
}
