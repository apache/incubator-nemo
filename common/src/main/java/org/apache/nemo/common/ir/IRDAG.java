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
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.PairKeyExtractor;
import org.apache.nemo.common.Util;
import org.apache.nemo.common.coder.BytesDecoderFactory;
import org.apache.nemo.common.coder.BytesEncoderFactory;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.dag.DAGInterface;
import org.apache.nemo.common.exception.CompileTimeOptimizationException;
import org.apache.nemo.common.exception.MetricException;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.executionproperty.ResourceSpecification;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.LoopVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.MessageIdVertexProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.utility.TaskSizeSplitterVertex;
import org.apache.nemo.common.ir.vertex.utility.runtimepass.MessageAggregatorVertex;
import org.apache.nemo.common.ir.vertex.utility.runtimepass.MessageGeneratorVertex;
import org.apache.nemo.common.ir.vertex.utility.RelayVertex;
import org.apache.nemo.common.ir.vertex.utility.SamplingVertex;
import org.apache.nemo.common.ir.vertex.utility.runtimepass.SignalVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * An IRDAG object captures a high-level data processing application (e.g., Spark/Beam application).
 * - IRVertex: A data-parallel operation. (e.g., map)
 * - IREdge: A data dependency between two operations. (e.g., shuffle)
 * <p>
 * Largely two types of IRDAG optimization(modification) methods are provided.
 * All of these methods preserve application semantics.
 * - Annotation: setProperty(), getPropertyValue() on each IRVertex/IREdge
 * - Reshaping: insert(), delete() on the IRDAG
 * <p>
 * TODO #341: Rethink IRDAG insert() signatures
 */
@NotThreadSafe
public final class IRDAG implements DAGInterface<IRVertex, IREdge> {
  private static final Logger LOG = LoggerFactory.getLogger(IRDAG.class.getName());

  private DAG<IRVertex, IREdge> dagSnapshot; // the DAG that was saved most recently.
  private DAG<IRVertex, IREdge> modifiedDAG; // the DAG that is being updated.

  // To remember original encoders/decoders, and etc
  private final Map<RelayVertex, IREdge> streamVertexToOriginalEdge;

  // To remember sampling vertex groups
  private final Map<SamplingVertex, Set<SamplingVertex>> samplingVertexToGroup;

  // To remember message barrier/aggregator vertex groups
  private final Map<IRVertex, Set<IRVertex>> messageVertexToGroup;

  /**
   * To remember the specifications of the executors used to run the IR DAG with.
   */
  private final List<Pair<Integer, ResourceSpecification>> executorInfo;

  /**
   * @param originalUserApplicationDAG the initial DAG.
   */
  public IRDAG(final DAG<IRVertex, IREdge> originalUserApplicationDAG) {
    this.modifiedDAG = originalUserApplicationDAG;
    this.dagSnapshot = originalUserApplicationDAG;
    this.streamVertexToOriginalEdge = new HashMap<>();
    this.samplingVertexToGroup = new HashMap<>();
    this.messageVertexToGroup = new HashMap<>();
    this.executorInfo = new ArrayList<>();
  }

  public IRDAGChecker.CheckerResult checkIntegrity() {
    return IRDAGChecker.get().doCheck(modifiedDAG);
  }

  //////////////////////////////////////////////////

  /**
   * Used internally by Nemo to advance the DAG snapshot after applying each pass.
   *
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

  /**
   * @return a IR DAG summary string, consisting of only the vertices generated from the frontend.
   */
  public String irDAGSummary() {
    final Long inputBytes = this.getInputSize();
    final String inputSizeString = inputBytes < 1024 ? inputBytes + "B"
      : (inputBytes / 1024 < 1024 ? inputBytes / 1024 + "KB"
      : (inputBytes / 1048576 < 1024 ? inputBytes / 1048576 + "MB"
      : (inputBytes / 1073741824L < 1024 ? inputBytes / 1073741824L + "GB"
      : inputBytes / 1099511627776L + "TB")));
    return "rv" + getRootVertices().size()
      + "_v" + getVertices().stream()
      .filter(v -> !v.isUtilityVertex())  // Exclude utility vertices
      .count()
      + "_e" + getVertices().stream()
      .filter(v -> !v.isUtilityVertex())  // Exclude utility vertices
      .mapToInt(v -> getIncomingEdgesOf(v).size())
      .sum() + "_" + inputSizeString;
  }

  /**
   * @return the total sum of the input size for the IR DAG.
   */
  public Long getInputSize() {
    return this.getRootVertices().stream()
      .filter(irVertex -> irVertex instanceof SourceVertex)
      .mapToLong(srcVertex -> {
        try {
          return ((SourceVertex) srcVertex).getEstimatedSizeBytes();
        } catch (Exception e) {
          throw new MetricException(e);
        }
      })
      .sum();
  }

  /**
   * Setter for the executor specifications information.
   * @param parsedExecutorInfo executor information parsed for processing.
   */
  public void recordExecutorInfo(final List<Pair<Integer, ResourceSpecification>> parsedExecutorInfo) {
    executorInfo.addAll(parsedExecutorInfo);
  }

  /**
   * Getter for the executor specifications information.
   * @return the executor specifications information.
   */
  public List<Pair<Integer, ResourceSpecification>> getExecutorInfo() {
    return executorInfo;
  }

  ////////////////////////////////////////////////// Methods for reshaping the DAG topology.

  /**
   * Deletes a previously inserted utility vertex.
   * (e.g., TriggerVertex, RelayVertex, SamplingVertex)
   * <p>
   * Notice that the actual number of vertices that will be deleted after this call returns can be more than one.
   * We roll back the changes made with the previous insert(), while preserving application semantics.
   *
   * @param vertexToDelete to delete.
   */
  public void delete(final IRVertex vertexToDelete) {
    assertExistence(vertexToDelete);
    deleteRecursively(vertexToDelete, new HashSet<>());

    // Build again, with source/sink checks on
    modifiedDAG = rebuildExcluding(modifiedDAG, Collections.emptySet()).build();
  }

  private Set<IRVertex> getVertexGroupToDelete(final IRVertex vertexToDelete) {
    if (vertexToDelete instanceof RelayVertex) {
      return Sets.newHashSet(vertexToDelete);
    } else if (vertexToDelete instanceof SamplingVertex) {
      final Set<SamplingVertex> samplingVertexGroup = samplingVertexToGroup.get(vertexToDelete);
      final Set<IRVertex> converted = new HashSet<>(samplingVertexGroup.size());
      for (final IRVertex sv : samplingVertexGroup) {
        converted.add(sv); // explicit conversion to IRVertex is needed.. otherwise the compiler complains :(
      }
      return converted;
    } else if (vertexToDelete instanceof MessageAggregatorVertex || vertexToDelete instanceof MessageGeneratorVertex) {
      return messageVertexToGroup.get(vertexToDelete);
    } else if (vertexToDelete instanceof SignalVertex) {
      return Sets.newHashSet(vertexToDelete);
    } else if (vertexToDelete instanceof TaskSizeSplitterVertex) {
      return Sets.newHashSet(vertexToDelete);
    } else {
      throw new IllegalArgumentException(vertexToDelete.getId());
    }
  }

  /**
   * Delete a group of vertex that corresponds to the specified vertex.
   * And then recursively delete neighboring utility vertices.
   * <p>
   * (WARNING) Only call this method inside delete(), or inside this method itself.
   * This method uses buildWithoutSourceSinkCheck() for intermediate DAGs,
   * which will be finally checked in delete().
   *
   * @param vertexToDelete to delete
   * @param visited        vertex groups (because cyclic dependencies between vertex groups are possible)
   */
  private void deleteRecursively(final IRVertex vertexToDelete, final Set<IRVertex> visited) {
    if (!Util.isUtilityVertex(vertexToDelete)) {
      throw new IllegalArgumentException(vertexToDelete.getId());
    }
    if (visited.contains(vertexToDelete)) {
      return;
    }

    // Three data structures
    final Set<IRVertex> vertexGroupToDelete = getVertexGroupToDelete(vertexToDelete);
    final Set<IRVertex> utilityParents = vertexGroupToDelete.stream()
      .map(modifiedDAG::getIncomingEdgesOf)
      .flatMap(inEdgeList -> inEdgeList.stream().map(IREdge::getSrc))
      .filter(Util::isUtilityVertex)
      .collect(Collectors.toSet());
    final Set<IRVertex> utilityChildren = vertexGroupToDelete.stream()
      .map(modifiedDAG::getOutgoingEdgesOf)
      .flatMap(outEdgeList -> outEdgeList.stream().map(IREdge::getDst))
      .filter(Util::isUtilityVertex)
      .collect(Collectors.toSet());

    // We have 'visited' this group
    visited.addAll(vertexGroupToDelete);

    // STEP 1: Delete parent utility vertices
    // Vertices that are 'in between' the group are also deleted here
    Sets.difference(utilityParents, vertexGroupToDelete).forEach(ptd -> deleteRecursively(ptd, visited));

    // STEP 2: Delete the specified vertex(vertices)
    if (vertexToDelete instanceof RelayVertex) {
      final DAGBuilder<IRVertex, IREdge> builder = rebuildExcluding(modifiedDAG, vertexGroupToDelete);

      // Add a new edge that directly connects the src of the stream vertex to its dst
      modifiedDAG.getOutgoingEdgesOf(vertexToDelete).stream()
        .filter(e -> !Util.isControlEdge(e))
        .map(IREdge::getDst)
        .forEach(dstVertex ->
          modifiedDAG.getIncomingEdgesOf(vertexToDelete).stream()
            .filter(e -> !Util.isControlEdge(e))
            .map(IREdge::getSrc)
            .forEach(srcVertex -> builder.connectVertices(
              Util.cloneEdge(streamVertexToOriginalEdge.get(vertexToDelete), srcVertex, dstVertex))));
      modifiedDAG = builder.buildWithoutSourceSinkCheck();
    } else if (vertexToDelete instanceof MessageAggregatorVertex || vertexToDelete instanceof MessageGeneratorVertex) {
      modifiedDAG = rebuildExcluding(modifiedDAG, vertexGroupToDelete).buildWithoutSourceSinkCheck();
      final Optional<Integer> deletedMessageIdOptional = vertexGroupToDelete.stream()
        .filter(vtd -> vtd instanceof MessageAggregatorVertex)
        .map(vtd -> vtd.getPropertyValue(MessageIdVertexProperty.class).<IllegalArgumentException>orElseThrow(
          () -> new IllegalArgumentException(
            "MessageAggregatorVertex " + vtd.getId() + " does not have MessageIdVertexProperty.")))
        .findAny();
      deletedMessageIdOptional.ifPresent(deletedMessageId ->
        modifiedDAG.getEdges().forEach(e ->
          e.getPropertyValue(MessageIdEdgeProperty.class).ifPresent(
            hashSet -> hashSet.remove(deletedMessageId))));
    } else if (vertexToDelete instanceof SamplingVertex) {
      modifiedDAG = rebuildExcluding(modifiedDAG, vertexGroupToDelete).buildWithoutSourceSinkCheck();
    } else if (vertexToDelete instanceof SignalVertex) {
      modifiedDAG = rebuildExcluding(modifiedDAG, vertexGroupToDelete).buildWithoutSourceSinkCheck();
      final Optional<Integer> deletedMessageIdOptional = vertexGroupToDelete.stream()
        .map(vtd -> vtd.getPropertyValue(MessageIdVertexProperty.class).<IllegalArgumentException>orElseThrow(
          () -> new IllegalArgumentException(
            "SignalVertex " + vtd.getId() + " does not have MessageIdVertexProperty.")))
        .findAny();
      deletedMessageIdOptional.ifPresent(deletedMessageId ->
        modifiedDAG.getEdges().forEach(e ->
          e.getPropertyValue(MessageIdEdgeProperty.class).ifPresent(
            hashSet -> hashSet.remove(deletedMessageId))));
    } else if (vertexToDelete instanceof TaskSizeSplitterVertex) {
      modifiedDAG = rebuildExcludingSplitter(modifiedDAG, vertexGroupToDelete).buildWithoutSourceSinkCheck();
    } else {
      throw new IllegalArgumentException(vertexToDelete.getId());
    }

    // STEP 3: Delete children utility vertices
    Sets.difference(utilityChildren, vertexGroupToDelete).forEach(ctd -> deleteRecursively(ctd, visited));
  }

  private DAGBuilder<IRVertex, IREdge> rebuildExcluding(final DAG<IRVertex, IREdge> dag, final Set<IRVertex> excluded) {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
    dag.getVertices().stream().filter(v -> !excluded.contains(v)).forEach(builder::addVertex);
    dag.getEdges().stream().filter(e -> !excluded.contains(e.getSrc()) && !excluded.contains(e.getDst()))
      .forEach(builder::connectVertices);
    return builder;
  }

  /**
   * helper method in deleting splitter vertex.
   * @param dag       dag to get information.
   * @param excluded  Set of Splitter vertex to delete. Always a singleton set.
   * @return
   */
  private DAGBuilder<IRVertex, IREdge> rebuildExcludingSplitter(final DAG<IRVertex, IREdge> dag,
                                                                final Set<IRVertex> excluded) {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
    dag.getVertices().stream().filter(v -> !excluded.contains(v)).forEach(builder::addVertex);
    dag.getEdges().stream()
      .filter(e -> !(excluded.contains(e.getSrc()) || excluded.contains(e.getDst())))
      .forEach(builder::connectVertices);

    for (IRVertex vertex : excluded) {
      if (!(vertex instanceof TaskSizeSplitterVertex)) {
        break;
      }
      final TaskSizeSplitterVertex splitter = (TaskSizeSplitterVertex) vertex;
      //first, restore original vertices
      DAG<IRVertex, IREdge> internalDag = splitter.getDAG();
      internalDag.getVertices().stream().filter(v -> !(v instanceof SignalVertex)).forEach(builder::addVertex);
      internalDag.getEdges().stream()
        .filter(e -> !(e.getSrc() instanceof SignalVertex || e.getDst() instanceof SignalVertex))
        .forEach(builder::connectVertices);

      //second, take care of edges connected to splitter vertex
      for (IREdge edgeToSplitter : dag.getIncomingEdgesOf(splitter)) {
        if (edgeToSplitter.getSrc() instanceof TaskSizeSplitterVertex) {
          final TaskSizeSplitterVertex prevSp = (TaskSizeSplitterVertex) edgeToSplitter.getSrc();
          final IREdge internalEdge = prevSp.getEdgeWithInternalVertex(edgeToSplitter);
          final IREdge newEdgeToPrevSp = Util.cloneEdge(internalEdge, prevSp, internalEdge.getDst());
          prevSp.mapEdgeWithLoop(newEdgeToPrevSp, internalEdge);

          builder.connectVertices(newEdgeToPrevSp);
        } else {
          final IREdge internalEdge = splitter.getEdgeWithInternalVertex(edgeToSplitter);
          builder.connectVertices(internalEdge);
        }
      }

      for (IREdge edgeFromSplitter : dag.getOutgoingEdgesOf(splitter)) {
        if (edgeFromSplitter.getDst() instanceof TaskSizeSplitterVertex) {
          final TaskSizeSplitterVertex nextSp = (TaskSizeSplitterVertex) edgeFromSplitter.getDst();
          final IREdge internalEdge = nextSp.getEdgeWithInternalVertex(edgeFromSplitter);
          final IREdge newEdgeToNextSp = Util.cloneEdge(internalEdge, internalEdge.getSrc(), nextSp);
          nextSp.mapEdgeWithLoop(newEdgeToNextSp, internalEdge);

          builder.connectVertices(newEdgeToNextSp);
        } else {
          final IREdge internalEdge = splitter.getEdgeWithInternalVertex(edgeFromSplitter);
          builder.connectVertices(internalEdge);
        }
      }
    }
    return builder;
  }

  /**
   * Inserts a new vertex that streams data.
   * <p>
   * Before: src - edgeToStreamize - dst
   * After: src - edgeToStreamizeWithNewDestination - relayVertex - oneToOneEdge - dst
   * (replaces the "Before" relationships)
   * <p>
   * This preserves semantics as the relayVertex simply forwards data elements from the input edge to the output edge.
   *
   * @param relayVertex    to insert.
   * @param edgeToStreamize to modify.
   */
  public void insert(final RelayVertex relayVertex, final IREdge edgeToStreamize) {
    assertNonExistence(relayVertex);
    assertNonControlEdge(edgeToStreamize);

    // Create a completely new DAG with the vertex inserted.
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();

    // Integrity check
    if (edgeToStreamize.getPropertyValue(MessageIdEdgeProperty.class).isPresent()
      && !edgeToStreamize.getPropertyValue(MessageIdEdgeProperty.class).get().isEmpty()) {
      throw new CompileTimeOptimizationException(edgeToStreamize.getId() + " has a MessageId, and cannot be removed");
    }

    // RelayVertex should not be inserted before SplitterVertex.
    if (edgeToStreamize.getDst() instanceof TaskSizeSplitterVertex) {
      return;
    }

    // Insert the vertex.
    final IRVertex vertexToInsert = wrapSamplingVertexIfNeeded(relayVertex, edgeToStreamize.getSrc());
    builder.addVertex(vertexToInsert);
    edgeToStreamize.getSrc().getPropertyValue(ParallelismProperty.class)
      .ifPresent(p -> vertexToInsert.setProperty(ParallelismProperty.of(p)));

    // Build the new DAG to reflect the new topology.
    modifiedDAG.topologicalDo(v -> {
      builder.addVertex(v); // None of the existing vertices are deleted.

      for (final IREdge edge : modifiedDAG.getIncomingEdgesOf(v)) {
        if (edge.equals(edgeToStreamize)) {
          // MATCH!

          // Edge to the relayVertex
          final IREdge toSV = new IREdge(
            edgeToStreamize.getPropertyValue(CommunicationPatternProperty.class).get(),
            edgeToStreamize.getSrc(),
            vertexToInsert);
          edgeToStreamize.copyExecutionPropertiesTo(toSV);

          // Edge from the relayVertex.
          final IREdge fromSV = new IREdge(CommunicationPatternProperty.Value.ONE_TO_ONE, vertexToInsert, v);
          fromSV.setProperty(EncoderProperty.of(edgeToStreamize.getPropertyValue(EncoderProperty.class).get()));
          fromSV.setProperty(DecoderProperty.of(edgeToStreamize.getPropertyValue(DecoderProperty.class).get()));

          // Annotations for efficient data transfers - toSV
          toSV.setPropertyPermanently(DecoderProperty.of(BytesDecoderFactory.of()));
          toSV.setPropertyPermanently(CompressionProperty.of(CompressionProperty.Value.LZ4));
          toSV.setPropertyPermanently(DecompressionProperty.of(CompressionProperty.Value.NONE));

          // Annotations for efficient data transfers - fromSV
          fromSV.setPropertyPermanently(EncoderProperty.of(BytesEncoderFactory.of()));
          fromSV.setPropertyPermanently(CompressionProperty.of(CompressionProperty.Value.NONE));
          fromSV.setPropertyPermanently(DecompressionProperty.of(CompressionProperty.Value.LZ4));
          fromSV.setPropertyPermanently(PartitionerProperty.of(PartitionerProperty.Type.DEDICATED_KEY_PER_ELEMENT));

          // Track the new edges.
          builder.connectVertices(toSV);
          builder.connectVertices(fromSV);
        } else {
          // NO MATCH, so simply connect vertices as before.
          builder.connectVertices(edge);
        }
      }
    });

    if (edgeToStreamize.getSrc() instanceof RelayVertex) {
      streamVertexToOriginalEdge.put(relayVertex, streamVertexToOriginalEdge.get(edgeToStreamize.getSrc()));
    } else if (edgeToStreamize.getDst() instanceof RelayVertex) {
      streamVertexToOriginalEdge.put(relayVertex, streamVertexToOriginalEdge.get(edgeToStreamize.getDst()));
    } else {
      streamVertexToOriginalEdge.put(relayVertex, edgeToStreamize);
    }
    modifiedDAG = builder.build(); // update the DAG.
  }

  /**
   * Inserts a new vertex that analyzes intermediate data, and triggers a dynamic optimization.
   * <p>
   * For each edge in edgesToGetStatisticsOf...
   * <p>
   * Before: src - edge - dst
   * After: src - oneToOneEdge(a clone of edge) - triggerVertex -
   * shuffleEdge - messageAggregatorVertex - broadcastEdge - dst
   * (the "Before" relationships are unmodified)
   * <p>
   * This preserves semantics as the results of the inserted message vertices are never consumed by the original IRDAG.
   * <p>
   * TODO #345: Simplify insert(TriggerVertex)
   *
   * @param messageGeneratorVertex    to insert.
   * @param messageAggregatorVertex to insert.
   * @param triggerOutputEncoder        to use.
   * @param triggerOutputDecoder        to use.
   * @param edgesToGetStatisticsOf  to examine.
   * @param edgesToOptimize         to optimize.
   */
  public void insert(final MessageGeneratorVertex messageGeneratorVertex,
                     final MessageAggregatorVertex messageAggregatorVertex,
                     final EncoderProperty triggerOutputEncoder,
                     final DecoderProperty triggerOutputDecoder,
                     final Set<IREdge> edgesToGetStatisticsOf,
                     final Set<IREdge> edgesToOptimize) {
    //edge case: when the destination of mav is splitter, do not insert!
    assertNonExistence(messageGeneratorVertex);
    assertNonExistence(messageAggregatorVertex);
    edgesToGetStatisticsOf.forEach(this::assertNonControlEdge);
    edgesToOptimize.forEach(this::assertNonControlEdge);

    if (edgesToGetStatisticsOf.stream().map(edge -> edge.getDst().getId()).collect(Collectors.toSet()).size() != 1) {
      throw new IllegalArgumentException("Not destined to the same vertex: " + edgesToOptimize.toString());
    }
    if (edgesToOptimize.stream().map(edge -> edge.getDst().getId()).collect(Collectors.toSet()).size() != 1) {
      throw new IllegalArgumentException("Not destined to the same vertex: " + edgesToOptimize.toString());
    }

    // Create a completely new DAG with the vertex inserted.
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();

    // All of the existing vertices and edges remain intact
    modifiedDAG.topologicalDo(v -> {
      builder.addVertex(v);
      modifiedDAG.getIncomingEdgesOf(v).forEach(builder::connectVertices);
    });

    ////////////////////////////////// STEP 1: Insert new vertices and edges (src - trigger - agg - dst)

    // From src to trigger
    final List<IRVertex> triggerList = new ArrayList<>();
    for (final IREdge edge : edgesToGetStatisticsOf) {
      final IRVertex triggerToAdd = wrapSamplingVertexIfNeeded(
        new MessageGeneratorVertex<>(messageGeneratorVertex.getMessageFunction()), edge.getSrc());
      builder.addVertex(triggerToAdd);
      triggerList.add(triggerToAdd);
      edge.getSrc().getPropertyValue(ParallelismProperty.class)
        .ifPresent(p -> triggerToAdd.setProperty(ParallelismProperty.of(p)));

      final IREdge edgeToClone;
      if (edge.getSrc() instanceof RelayVertex) {
        edgeToClone = streamVertexToOriginalEdge.get(edge.getSrc());
      } else if (edge.getDst() instanceof RelayVertex) {
        edgeToClone = streamVertexToOriginalEdge.get(edge.getDst());
      } else {
        edgeToClone = edge;
      }

      final IREdge clone = Util.cloneEdge(
        CommunicationPatternProperty.Value.ONE_TO_ONE, edgeToClone, edge.getSrc(), triggerToAdd);
      if (edge.getSrc() instanceof TaskSizeSplitterVertex) {
        builder.connectSplitterVertexWithoutReplacing(edgeToClone, clone);
      } else {
        builder.connectVertices(clone);
      }
    }

    // Add agg (no need to wrap inside sampling vertices)
    builder.addVertex(messageAggregatorVertex);

    // From trigger to agg
    for (final IRVertex trigger : triggerList) {
      final IREdge edgeToMav = edgeToMessageAggregator(
        trigger, messageAggregatorVertex, triggerOutputEncoder, triggerOutputDecoder);
      builder.connectVertices(edgeToMav);
    }

    // From agg to dst
    // Add a control dependency (no output) from the messageAggregatorVertex to the destination.
    IREdge aggToDst = Util.createControlEdge(
      messageAggregatorVertex, edgesToGetStatisticsOf.iterator().next().getDst());
    if (edgesToGetStatisticsOf.iterator().next().getDst() instanceof TaskSizeSplitterVertex) {
      builder.connectSplitterVertexWithoutReplacing(edgesToGetStatisticsOf.iterator().next(), aggToDst);
    } else {
      builder.connectVertices(aggToDst);
    }


    ////////////////////////////////// STEP 2: Annotate the MessageId on optimization target edges

    modifiedDAG.topologicalDo(v ->
      modifiedDAG.getIncomingEdgesOf(v).forEach(inEdge -> {
        if (edgesToOptimize.contains(inEdge)) {
          final HashSet<Integer> msgEdgeIds =
            inEdge.getPropertyValue(MessageIdEdgeProperty.class).orElse(new HashSet<>(0));
          msgEdgeIds.add(messageAggregatorVertex.getPropertyValue(MessageIdVertexProperty.class).get());
          inEdge.setProperty(MessageIdEdgeProperty.of(msgEdgeIds));
        }
      })
    );

    final Set<IRVertex> insertedVertices = new HashSet<>();
    insertedVertices.addAll(triggerList);
    insertedVertices.add(messageAggregatorVertex);
    triggerList.forEach(trigger -> messageVertexToGroup.put(trigger, insertedVertices));
    messageVertexToGroup.put(messageAggregatorVertex, insertedVertices);

    modifiedDAG = builder.build(); // update the DAG.
  }

  /**
   * Inserts new vertex which calls for runtime pass.
   *
   * e.g) suppose that we want to change vertex 2's property by using runtime pass, but the related data is not gained
   * directly from the incoming edge of vertex 2 (for example, the data is gained from using simulation).
   * In this case, it is unnecessary to insert message generator vertex and message aggregator vertex to launch runtime
   * pass.
   *
   * Original case: (vertex1) -- shuffle edge -- (vertex 2)
   *
   * After inserting signal Vertex:
   * (vertex 1) -------------------- shuffle edge ------------------- (vertex 2)
   *            -- control edge -- (signal vertex) -- control edge --
   *
   * Therefore, the shuffle edge to vertex 2 is executed after signal vertex is executed.
   * Since signal vertex only 'signals' the launch of runtime pass, its parallelism is sufficient to be only 1.
   * @param toInsert        Signal vertex to optimize.
   * @param edgeToOptimize  Original edge to optimize(in the above example, shuffle edge).
   */
  public void insert(final SignalVertex toInsert,
                     final IREdge edgeToOptimize) {

    // Create a completely new DAG with the vertex inserted.
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();

    // All of the existing vertices and edges remain intact
    modifiedDAG.topologicalDo(v -> {
      builder.addVertex(v);
      modifiedDAG.getIncomingEdgesOf(v).forEach(builder::connectVertices);
    });

    // insert Signal Vertex in DAG.
    builder.addVertex(toInsert);

    final IREdge controlEdgeToSV = Util.createControlEdge(edgeToOptimize.getSrc(), toInsert);
    final IREdge controlEdgeFromSV = Util.createControlEdge(toInsert, edgeToOptimize.getDst());

    builder.connectVertices(controlEdgeToSV);
    builder.connectVertices(controlEdgeFromSV);

    modifiedDAG.topologicalDo(v ->
      modifiedDAG.getIncomingEdgesOf(v).forEach(inEdge -> {
        if (edgeToOptimize.equals(inEdge)) {
          final HashSet<Integer> msgEdgeIds =
            inEdge.getPropertyValue(MessageIdEdgeProperty.class).orElse(new HashSet<>(0));
          msgEdgeIds.add(toInsert.getPropertyValue(MessageIdVertexProperty.class).get());
          inEdge.setProperty(MessageIdEdgeProperty.of(msgEdgeIds));
        }
      })
    );
    // update the DAG.
    modifiedDAG = builder.build();
  }

  /**
   * Inserts a set of samplingVertices that process sampled data.
   * <p>
   * This method automatically inserts the following three types of edges.
   * (1) Edges between samplingVertices to reflect the original relationship
   * (2) Edges from the original IRDAG to samplingVertices that clone the inEdges of the original vertices
   * (3) Edges from the samplingVertices to the original IRDAG to respect executeAfterSamplingVertices
   * <p>
   * Suppose the caller supplies the following arguments to perform a "sampled run" of vertices {V1, V2},
   * prior to executing them.
   * - samplingVertices: {V1', V2'}
   * - childrenOfSamplingVertices: {V1}
   * <p>
   * Before: V1 - oneToOneEdge - V2 - shuffleEdge - V3
   * After: V1' - oneToOneEdge - V2' - controlEdge - V1 - oneToOneEdge - V2 - shuffleEdge - V3
   * <p>
   * This preserves semantics as the original IRDAG remains unchanged and unaffected.
   * <p>
   * (Future calls to insert() can add new vertices that connect to sampling vertices. Such new vertices will also be
   * wrapped with sampling vertices, as new vertices that consume outputs from sampling vertices will process
   * a subset of data anyways, and no such new vertex will reach the original DAG except via control edges)
   * <p>
   * TODO #343: Extend SamplingVertex control edges
   *
   * @param toInsert     sampling vertices.
   * @param executeAfter that must be executed after toInsert.
   */
  public void insert(final Set<SamplingVertex> toInsert,
                     final Set<IRVertex> executeAfter) {
    toInsert.forEach(this::assertNonExistence);
    executeAfter.forEach(this::assertExistence);

    // Create a completely new DAG with the vertex inserted.
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();

    // All of the existing vertices and edges remain intact
    modifiedDAG.topologicalDo(v -> {
      builder.addVertex(v);
      modifiedDAG.getIncomingEdgesOf(v).forEach(builder::connectVertices);
    });

    // Add the sampling vertices
    toInsert.forEach(builder::addVertex);

    // Get the original vertices
    final Map<IRVertex, IRVertex> originalToSampling = toInsert.stream()
      .collect(Collectors.toMap(sv -> modifiedDAG.getVertexById(sv.getOriginalVertexId()), Function.identity()));
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
    notBetweenOriginals.stream().map(nboEdge -> {
      final IREdge cloneEdge = Util.cloneEdge(
        nboEdge,
        nboEdge.getSrc(), // sampling vertices consume a subset of original data partitions here
        originalToSampling.get(nboEdge.getDst()));
      nboEdge.copyExecutionPropertiesTo(cloneEdge); // exec properties must be exactly the same
      return cloneEdge;
    }).forEach(builder::connectVertices);

    // [EDGE TYPE 3] From sampling vertices to vertices that should be executed after
    final Set<IRVertex> sinks = getSinksWithinVertexSet(modifiedDAG, originalToSampling.keySet())
      .stream()
      .map(originalToSampling::get)
      .collect(Collectors.toSet());
    for (final IRVertex ea : executeAfter) {
      for (final IRVertex sink : sinks) {
        // Control edge that enforces execution ordering
        builder.connectVertices(Util.createControlEdge(sink, ea));
      }
    }

    toInsert.forEach(tiv -> samplingVertexToGroup.put(tiv, toInsert));
    modifiedDAG = builder.build(); // update the DAG.
  }

  /**
   * Insert TaskSizeSplitterVertex in dag.
   * @param toInsert          TaskSizeSplitterVertex to insert.
   */
  public void insert(final TaskSizeSplitterVertex toInsert) {
    final Set<IRVertex> originalVertices = toInsert.getOriginalVertices();

    final Set<IREdge> incomingEdgesOfOriginalVertices = originalVertices
      .stream()
      .flatMap(ov -> modifiedDAG.getIncomingEdgesOf(ov).stream())
      .collect(Collectors.toSet());

    final Set<IREdge> outgoingEdgesOfOriginalVertices = originalVertices
      .stream()
      .flatMap(ov -> modifiedDAG.getOutgoingEdgesOf(ov).stream())
      .collect(Collectors.toSet());

    final Set<IREdge> fromOutsideToOriginal = toInsert.getEdgesFromOutsideToOriginal(modifiedDAG);
    final Set<IREdge> fromOriginalToOutside = toInsert.getEdgesFromOriginalToOutside(modifiedDAG);

    // make edges connected to splitter vertex
    final Set<IREdge> fromOutsideToSplitter = toInsert.getEdgesFromOutsideToSplitter(modifiedDAG);
    final Set<IREdge> fromSplitterToOutside = toInsert.getEdgesFromSplitterToOutside(modifiedDAG);

    //map splitter vertex connection to corresponding internal vertex connection
    for (IREdge splitterEdge : fromSplitterToOutside) {
      for (IREdge internalEdge : fromOriginalToOutside) {
        if (splitterEdge.getDst() instanceof TaskSizeSplitterVertex) {
          TaskSizeSplitterVertex nextSplitter = (TaskSizeSplitterVertex) splitterEdge.getDst();
          if (nextSplitter.getOriginalVertices().contains(internalEdge.getDst())) {
            toInsert.mapEdgeWithLoop(splitterEdge, internalEdge);
          }
        } else {
          if (splitterEdge.getDst().equals(internalEdge.getDst())) {
            toInsert.mapEdgeWithLoop(splitterEdge, internalEdge);
          }
        }
      }
    }

    for (IREdge splitterEdge : fromOutsideToSplitter) {
      for (IREdge internalEdge : fromOutsideToOriginal) {
        if (splitterEdge.getSrc().equals(internalEdge.getSrc())) {
          toInsert.mapEdgeWithLoop(splitterEdge, internalEdge);
        }
      }
    }

    fromOutsideToOriginal.forEach(toInsert::addDagIncomingEdge);
    fromOutsideToOriginal.forEach(toInsert::addNonIterativeIncomingEdge);
    fromOriginalToOutside.forEach(toInsert::addDagOutgoingEdge);

    // All preparation done. Insert splitter vertex.
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();

    //insert vertex and edges irrelevant to splitter vertex
    modifiedDAG.topologicalDo(v -> {
      if (!originalVertices.contains(v)) {
        builder.addVertex(v);
        for (IREdge edge : modifiedDAG.getIncomingEdgesOf(v)) {
          if (!incomingEdgesOfOriginalVertices.contains(edge) && !outgoingEdgesOfOriginalVertices.contains(edge)) {
            builder.connectVertices(edge);
          }
        }
      }
    });
    //insert splitter vertices
    builder.addVertex(toInsert);

    //connect splitter to outside world
    fromOutsideToSplitter.forEach(builder::connectVertices);
    fromSplitterToOutside.forEach(builder::connectVertices);

    modifiedDAG = builder.build();
  }

  /**
   * Reshape unsafely, without guarantees on preserving application semantics.
   * TODO #330: Refactor Unsafe Reshaping Passes
   *
   * @param unsafeReshapingFunction takes as input the underlying DAG, and outputs a reshaped DAG.
   */
  public void reshapeUnsafely(final Function<DAG<IRVertex, IREdge>, DAG<IRVertex, IREdge>> unsafeReshapingFunction) {
    modifiedDAG = unsafeReshapingFunction.apply(modifiedDAG);
  }

  ////////////////////////////////////////////////// Private helper methods.

  private Set<IRVertex> getSinksWithinVertexSet(final DAG<IRVertex, IREdge> dag,
                                                final Set<IRVertex> vertexSet) {
    final Set<IRVertex> parentsOfAnotherVertex = vertexSet.stream()
      .flatMap(v -> dag.getOutgoingEdgesOf(v).stream())
      .filter(e -> vertexSet.contains(e.getDst()))
      .map(IREdge::getSrc) // makes the result a subset of the input vertexSet
      .collect(Collectors.toSet());
    return Sets.difference(vertexSet, parentsOfAnotherVertex);
  }

  private IRVertex wrapSamplingVertexIfNeeded(final IRVertex newVertex, final IRVertex existingVertexToConnectWith) {
    // If the connecting vertex is a sampling vertex, the new vertex must be wrapped inside a sampling vertex too.
    return existingVertexToConnectWith instanceof SamplingVertex
      ? new SamplingVertex(newVertex, ((SamplingVertex) existingVertexToConnectWith).getDesiredSampleRate())
      : newVertex;
  }

  private void assertNonControlEdge(final IREdge e) {
    if (Util.isControlEdge(e)) {
      throw new IllegalArgumentException(e.getId());
    }
  }

  private void assertExistence(final IRVertex v) {
    if (!getVertices().contains(v)) {
      throw new IllegalArgumentException(v.getId());
    }
  }

  private void assertNonExistence(final IRVertex v) {
    if (getVertices().contains(v)) {
      throw new IllegalArgumentException(v.getId());
    }
  }

  /**
   * @param trigger src.
   * @param agg     dst.
   * @param encoder src-dst encoder.
   * @param decoder src-dst decoder.
   * @return the edge.
   */
  private IREdge edgeToMessageAggregator(final IRVertex trigger,
                                         final IRVertex agg,
                                         final EncoderProperty encoder,
                                         final DecoderProperty decoder) {
    final IREdge newEdge = new IREdge(CommunicationPatternProperty.Value.SHUFFLE, trigger, agg);
    newEdge.setProperty(DataStoreProperty.of(DataStoreProperty.Value.LOCAL_FILE_STORE));
    newEdge.setProperty(DataPersistenceProperty.of(DataPersistenceProperty.Value.KEEP));
    newEdge.setProperty(DataFlowProperty.of(DataFlowProperty.Value.PUSH));
    newEdge.setPropertyPermanently(encoder);
    newEdge.setPropertyPermanently(decoder);
    newEdge.setPropertyPermanently(KeyExtractorProperty.of(new PairKeyExtractor()));

    // TODO #345: Simplify insert(TriggerVertex)
    // these are obviously wrong, but hacks for now...
    newEdge.setPropertyPermanently(KeyEncoderProperty.of(encoder.getValue()));
    newEdge.setPropertyPermanently(KeyDecoderProperty.of(decoder.getValue()));

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
  public IREdge getEdgeById(final String id) {
    return modifiedDAG.getEdgeById(id);
  }

  @Override
  public List<IRVertex> getVertices() {
    return modifiedDAG.getVertices();
  }

  @Override
  public List<IREdge> getEdges() {
    return modifiedDAG.getEdges();
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
                               final String dstVertexId) {
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

  @Override
  public String toString() {
    return asJsonNode().toString();
  }
}
