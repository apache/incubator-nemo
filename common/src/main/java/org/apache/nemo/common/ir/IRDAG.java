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
import com.google.common.collect.Sets;
import org.apache.nemo.common.KeyExtractor;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.Util;
import org.apache.nemo.common.coder.BytesDecoderFactory;
import org.apache.nemo.common.coder.BytesEncoderFactory;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.dag.DAGInterface;
import org.apache.nemo.common.exception.IllegalEdgeOperationException;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.LoopVertex;
import org.apache.nemo.common.ir.vertex.utility.MessageAggregatorVertex;
import org.apache.nemo.common.ir.vertex.utility.MessageBarrierVertex;
import org.apache.nemo.common.ir.vertex.utility.SamplingVertex;
import org.apache.nemo.common.ir.vertex.utility.StreamVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
public final class IRDAG implements DAGInterface<IRVertex, IREdge> {
  private static final Logger LOG = LoggerFactory.getLogger(IRDAG.class.getName());

  private DAG<IRVertex, IREdge> dagSnapshot; // the DAG that was saved most recently.
  private DAG<IRVertex, IREdge> modifiedDAG; // the DAG that is being updated.

  /**
   * @param originalUserApplicationDAG the initial DAG.
   */
  public IRDAG(final DAG<IRVertex, IREdge> originalUserApplicationDAG) {
    this.modifiedDAG = originalUserApplicationDAG;
    this.dagSnapshot = originalUserApplicationDAG;
  }

  //////////////////////////////////////////////////

  /**
   * Used internally by Nemo to advance the DAG snapshot after applying each pass.
   * @param checker that compares the dagSnapshot and the modifiedDAG
   *                to determine if the snapshot can be set the current modifiedDAG.
   * @return true if the checker passes, false otherwise.
   */
  public boolean advanceDAGSnapshot(final BiFunction<IRDAG, IRDAG, Boolean> checker) {
    final boolean canAdvance = checker.apply(new IRDAG(dagSnapshot), new IRDAG(modifiedDAG));
    if (canAdvance) {
      dagSnapshot = modifiedDAG;
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
   * This preserves semantics as the streamVertex simply forwards data elements from the input edge to the output edge.
   *
   * @param streamVertex to insert.
   * @param edgeToStreamize to modify.
   */
  public void insert(final StreamVertex streamVertex, final IREdge edgeToStreamize) {
    // Create a completely new DAG with the vertex inserted.
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();

    // Insert the vertex.
    builder.addVertex(streamVertex);

    // Build the new DAG to reflect the new topology.
    modifiedDAG.topologicalDo(v -> {
      builder.addVertex(v); // None of the existing vertices are deleted.

      for (final IREdge edge : modifiedDAG.getIncomingEdgesOf(v)) {
        if (edge.equals(edgeToStreamize)) {
          // MATCH!

          // Edge to the streamVertex
          final IREdge toSV = new IREdge(
            edgeToStreamize.getPropertyValue(CommunicationPatternProperty.class).get(),
            edgeToStreamize.getSrc(),
            streamVertex);
          edgeToStreamize.copyExecutionPropertiesTo(toSV);

          // Edge from the streamVertex.
          final IREdge fromSV = new IREdge(CommunicationPatternProperty.Value.OneToOne, streamVertex, v);

          // Future optimizations may want to use the original encoders/compressions.
          toSV.setPropertySnapshot();
          fromSV.setPropertySnapshot();

          // Annotations for efficient data transfers - toSV
          toSV.setPropertyPermanently(DecoderProperty.of(BytesDecoderFactory.of()));
          toSV.setPropertyPermanently(CompressionProperty.of(CompressionProperty.Value.LZ4));
          toSV.setPropertyPermanently(DecompressionProperty.of(CompressionProperty.Value.None));

          // Annotations for efficient data transfers - fromSV
          fromSV.setPropertyPermanently(EncoderProperty.of(BytesEncoderFactory.of()));
          fromSV.setPropertyPermanently(CompressionProperty.of(CompressionProperty.Value.None));
          fromSV.setPropertyPermanently(DecompressionProperty.of(CompressionProperty.Value.LZ4));
          fromSV.setPropertyPermanently(PartitionerProperty.of(PartitionerProperty.Type.DedicatedKeyPerElement));

          // Track the new edges.
          builder.connectVertices(toSV);
          builder.connectVertices(fromSV);
        } else {
          // NO MATCH, so simply connect vertices as before.
          builder.connectVertices(edge);
        }
      }
    });

    modifiedDAG = builder.build(); // update the DAG.
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
   * This preserves semantics as the results of the inserted message vertices are never consumed by the original IRDAG.
   *
   * @param messageBarrierVertex to insert.
   * @param messageAggregatorVertex to insert.
   * @param mbvOutputEncoder to use.
   * @param mbvOutputDecoder to use.
   * @param edgesToGetStatisticsOf to examine.
   * @param edgesToOptimize to optimize.
   */
  public void insert(final MessageBarrierVertex messageBarrierVertex,
                     final MessageAggregatorVertex messageAggregatorVertex,
                     final EncoderProperty mbvOutputEncoder,
                     final DecoderProperty mbvOutputDecoder,
                     final Set<IREdge> edgesToGetStatisticsOf,
                     final Set<IREdge> edgesToOptimize) {
    // Create a completely new DAG with the vertex inserted.
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();

    // All of the existing vertices and edges remain intact
    modifiedDAG.topologicalDo(v -> {
      builder.addVertex(v);
      modifiedDAG.getIncomingEdgesOf(v).forEach(builder::connectVertices);
    });

    ////////////////////////////////// STEP 1: Insert new vertices and edges

    // From mav to dst
    // Add a control dependency (no output) from the messageAggregatorVertex to the destination.
    builder.addVertex(messageAggregatorVertex);
    final IRVertex dst = edgesToGetStatisticsOf.iterator().next().getDst();
    builder.connectVertices(Util.createControlEdge(messageAggregatorVertex, dst));

    // Build the edges: src - mbv - mav
    for (final IREdge edge : edgesToGetStatisticsOf) {
      final MessageBarrierVertex mbv = new MessageBarrierVertex<>(messageBarrierVertex.getMessageFunction());
      builder.addVertex(mbv);

      // From src to mbv
      final IREdge clone = Util.cloneEdge(CommunicationPatternProperty.Value.OneToOne, edge, edge.getSrc(), mbv);
      builder.connectVertices(clone);

      // From mbv to mav
      final IREdge edgeToABV = edgeBetweenMessageVertices(
        mbv, messageAggregatorVertex, mbvOutputEncoder, mbvOutputDecoder);
      builder.connectVertices(edgeToABV);
    }

    ////////////////////////////////// STEP 2: Annotate the MessageId on optimization target edges

    if (edgesToOptimize.stream().map(edge -> edge.getDst().getId()).collect(Collectors.toSet()).size() != 1) {
      throw new IllegalArgumentException("Not destined to the same vertex: " + edgesToOptimize.toString());
    }
    modifiedDAG.topologicalDo(v -> {
      modifiedDAG.getIncomingEdgesOf(v).forEach(inEdge -> {
        if (edgesToOptimize.contains(inEdge)) {
          inEdge.setPropertyPermanently(MessageIdProperty.of(messageAggregatorVertex.getMessageId()));
        }
      });
    });

    modifiedDAG = builder.build(); // update the DAG.
  }

  /**
   * Inserts a set of samplingVertices that process sampled data.
   *
   * This method automatically inserts the following three types of edges.
   * (1) Edges between samplingVertices to reflect the original relationship
   * (2) Edges from the original IRDAG to samplingVertices that clone the inEdges of the original vertices
   * (3) Edges from the samplingVertices to the original IRDAG to respect executeAfterSamplingVertices
   *
   * Suppose the caller supplies the following arguments to perform a "sampled run" of vertices {V1, V2},
   * prior to executing them.
   * - samplingVertices: {V1', V2'}
   * - childrenOfSamplingVertices: {V1}
   *
   * Before: V1 - oneToOneEdge - V2 - shuffleEdge - V3
   * After: V1' - oneToOneEdge - V2' - controlEdge - V1 - oneToOneEdge - V2 - shuffleEdge - V3
   *
   * This preserves semantics as the original IRDAG remains unchanged and unaffected.
   *
   * @param samplingVertices to insert.
   * @param executeAfterSamplingVertices that must be executed after samplingVertices.
   */
  public void insert(final Set<SamplingVertex> samplingVertices,
                     final Set<IRVertex> executeAfterSamplingVertices) {
    LOG.info("samplingVertices {} / executeAfterSamplingVertices {}", samplingVertices, executeAfterSamplingVertices);

    // Create a completely new DAG with the vertex inserted.
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();

    // All of the existing vertices and edges remain intact
    modifiedDAG.topologicalDo(v -> {
      builder.addVertex(v);
      modifiedDAG.getIncomingEdgesOf(v).forEach(builder::connectVertices);
    });

    // Add the sampling vertices
    samplingVertices.forEach(builder::addVertex);

    // Get the original vertices
    final Map<IRVertex, IRVertex> originalToSampling = samplingVertices.stream()
      .collect(Collectors.toMap(SamplingVertex::getOriginalVertex, Function.identity()));
    final Set<IREdge> inEdgesOfOriginals = originalToSampling.keySet()
      .stream()
      .flatMap(ov -> modifiedDAG.getIncomingEdgesOf(ov).stream())
      .collect(Collectors.toSet());

    // [EDGE TYPE 1] Between sampling vertices
    final Set<IREdge> betweenOriginals = inEdgesOfOriginals
      .stream()
      .filter(ovInEdge -> originalToSampling.containsKey(ovInEdge.getSrc()))
      .collect(Collectors.toSet());
    betweenOriginals.stream().map(boEdge -> Util.cloneEdge(
      boEdge,
      originalToSampling.get(boEdge.getSrc()),
      originalToSampling.get(boEdge.getDst()))).forEach(builder::connectVertices);

    // [EDGE TYPE 2] From original IRDAG to sampling vertices
    final Set<IREdge> notBetweenOriginals = inEdgesOfOriginals
      .stream()
      .filter(ovInEdge -> !originalToSampling.containsKey(ovInEdge.getSrc()))
      .collect(Collectors.toSet());
    notBetweenOriginals.stream().map(nboEdge -> Util.cloneEdge(
      nboEdge,
      nboEdge.getSrc(), // sampling vertices consume a subset of original data partitions here
      originalToSampling.get(nboEdge.getDst()))).forEach(builder::connectVertices);

    // [EDGE TYPE 3] From sampling vertices to vertices that should be executed after
    final Set<IRVertex> parentsOfAnotherOriginal = betweenOriginals.stream()
      .map(IREdge::getSrc)
      .collect(Collectors.toSet());
    final Set<IRVertex> sinks = Sets.difference(originalToSampling.keySet(), parentsOfAnotherOriginal)
      .stream()
      .map(originalToSampling::get)
      .collect(Collectors.toSet());
    for (final IRVertex executeAfter : executeAfterSamplingVertices) {
      for (final IRVertex sink : sinks) {
        // Control edge that enforces execution ordering
        builder.connectVertices(Util.createControlEdge(sink, executeAfter));
      }
    }

    modifiedDAG = builder.build(); // update the DAG.
  }

  /**
   * Reshape unsafely, without guarantees on preserving application semantics.
   * TODO #330: Refactor Unsafe Reshaping Passes
   * @param unsafeReshapingFunction takes as input the underlying DAG, and outputs a reshaped DAG.
   */
  public void reshapeUnsafely(final Function<DAG<IRVertex, IREdge>, DAG<IRVertex, IREdge>> unsafeReshapingFunction) {
    modifiedDAG = unsafeReshapingFunction.apply(modifiedDAG);
  }

  ////////////////////////////////////////////////// Private helper methods.

  /**
   * @param mbv src.
   * @param mav dst.
   * @param encoder src-dst encoder.
   * @param decoder src-dst decoder.
   * @return the edge.
   */
  private IREdge edgeBetweenMessageVertices(final MessageBarrierVertex mbv,
                                            final MessageAggregatorVertex mav,
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

  ////////////////////////////////////////////////// DAGInterface methods - forward calls to the underlying DAG.

  @Override
  public void topologicalDo(final Consumer<IRVertex> function) {
    modifiedDAG.topologicalDo(function);
  }

  @Override
  public void dfsTraverse(final Consumer<IRVertex> function, final TraversalOrder traversalOrder) {
    modifiedDAG.dfsTraverse(function, traversalOrder);
  }

  @Override
  public void dfsDo(final IRVertex vertex,
                    final Consumer<IRVertex> vertexConsumer,
                    final TraversalOrder traversalOrder,
                    final Set<IRVertex> visited) {
    modifiedDAG.dfsDo(vertex, vertexConsumer, traversalOrder, visited);
  }

  @Override
  public Boolean pathExistsBetween(final IRVertex v1, final IRVertex v2) {
    return modifiedDAG.pathExistsBetween(v1, v2);
  }

  @Override
  public Boolean isCompositeVertex(final IRVertex irVertex) {
    return modifiedDAG.isCompositeVertex(irVertex);
  }

  @Override
  public Integer getLoopStackDepthOf(final IRVertex irVertex) {
    return modifiedDAG.getLoopStackDepthOf(irVertex);
  }

  @Override
  public LoopVertex getAssignedLoopVertexOf(final IRVertex irVertex) {
    return modifiedDAG.getAssignedLoopVertexOf(irVertex);
  }

  @Override
  public ObjectNode asJsonNode() {
    return modifiedDAG.asJsonNode();
  }

  @Override
  public void storeJSON(final String directory, final String name, final String description) {
    modifiedDAG.storeJSON(directory, name, description);
  }

  @Override
  public IRVertex getVertexById(final String id) {
    return modifiedDAG.getVertexById(id);
  }

  @Override
  public List<IRVertex> getVertices() {
    return modifiedDAG.getVertices();
  }

  @Override
  public List<IRVertex> getRootVertices() {
    return modifiedDAG.getRootVertices();
  }

  @Override
  public List<IREdge> getIncomingEdgesOf(final IRVertex v) {
    return modifiedDAG.getIncomingEdgesOf(v);
  }

  @Override
  public List<IREdge> getIncomingEdgesOf(final String vertexId) {
    return modifiedDAG.getIncomingEdgesOf(vertexId);
  }

  @Override
  public List<IREdge> getOutgoingEdgesOf(final IRVertex v) {
    return modifiedDAG.getOutgoingEdgesOf(v);
  }

  @Override
  public List<IREdge> getOutgoingEdgesOf(final String vertexId) {
    return modifiedDAG.getOutgoingEdgesOf(vertexId);
  }

  @Override
  public List<IRVertex> getParents(final String vertexId) {
    return modifiedDAG.getParents(vertexId);
  }

  @Override
  public List<IRVertex> getChildren(final String vertexId) {
    return modifiedDAG.getChildren(vertexId);
  }

  @Override
  public IREdge getEdgeBetween(final String srcVertexId,
                               final String dstVertexId) throws IllegalEdgeOperationException {
    return modifiedDAG.getEdgeBetween(srcVertexId, dstVertexId);
  }

  @Override
  public List<IRVertex> getTopologicalSort() {
    return modifiedDAG.getTopologicalSort();
  }

  @Override
  public List<IRVertex> getAncestors(final String vertexId) {
    return modifiedDAG.getAncestors(vertexId);
  }

  @Override
  public List<IRVertex> getDescendants(final String vertexId) {
    return modifiedDAG.getDescendants(vertexId);
  }

  @Override
  public List<IRVertex> filterVertices(final Predicate<IRVertex> condition) {
    return modifiedDAG.filterVertices(condition);
  }

  ////////////////////////////////////////////////// Utility methods for debugging

  public static String stringifyIRVertexIds(final Collection<IRVertex> vertices) {
    return vertices.stream().map(IRVertex::getId).collect(Collectors.toSet()).toString();
  }

  public static String stringifyIREdgeIds(final Collection<IREdge> edges) {
    return edges.stream().map(IREdge::getId).collect(Collectors.toSet()).toString();
  }
}
