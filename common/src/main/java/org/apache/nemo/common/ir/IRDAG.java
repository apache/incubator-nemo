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
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.dag.DAGInterface;
import org.apache.nemo.common.exception.CompileTimeOptimizationException;
import org.apache.nemo.common.exception.IllegalEdgeOperationException;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.LoopVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.MessageIdVertexProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.transform.CRTransform;
import org.apache.nemo.common.ir.vertex.utility.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
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
 *
 * TODO #341: Rethink IRDAG insert() signatures
 */
@NotThreadSafe
public final class IRDAG implements DAGInterface<IRVertex, IREdge> {
  private static final Logger LOG = LoggerFactory.getLogger(IRDAG.class.getName());

  private DAG<IRVertex, IREdge> dagSnapshot; // the DAG that was saved most recently.
  private DAG<IRVertex, IREdge> modifiedDAG; // the DAG that is being updated.

  // To remember original encoders/decoders, and etc
  // private transient final Map<OperatorVertex, IREdge> streamVertexToOriginalEdge;

  // To remember sampling vertex groups
  private transient final Map<SamplingVertex, Set<SamplingVertex>> samplingVertexToGroup;

  // To remember message barrier/aggregator vertex groups
  private transient final Map<IRVertex, Set<IRVertex>> messageVertexToGroup;


  /**
   * @param originalUserApplicationDAG the initial DAG.
   */
  public IRDAG(final DAG<IRVertex, IREdge> originalUserApplicationDAG) {
    this.modifiedDAG = originalUserApplicationDAG;
    this.dagSnapshot = originalUserApplicationDAG;
    // this.streamVertexToOriginalEdge = new HashMap<>();
    this.samplingVertexToGroup = new HashMap<>();
    this.messageVertexToGroup = new HashMap<>();
  }

  public IRDAGChecker.CheckerResult checkIntegrity() {
    return IRDAGChecker.get().doCheck(modifiedDAG);
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

  public String irDAGSummary() {
    return "RV" + getRootVertices().size() + "_V" + getVertices().size() + "_E" + getVertices().stream()
      .mapToInt(v -> getIncomingEdgesOf(v).size())
      .sum();
  }

  ////////////////////////////////////////////////// Methods for reshaping the DAG topology.

  /**
   * Deletes a previously inserted utility vertex.
   * (e.g., MessageBarrierVertex, StreamVertex, SamplingVertex)
   *
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
    if (vertexToDelete instanceof StreamVertex) {
      return Sets.newHashSet(vertexToDelete);
    } else if (vertexToDelete instanceof SamplingVertex) {
      final Set<SamplingVertex> samplingVertexGroup = samplingVertexToGroup.get(vertexToDelete);
      final Set<IRVertex> converted = new HashSet<>(samplingVertexGroup.size());
      for (final IRVertex sv : samplingVertexGroup) {
        converted.add(sv); // explicit conversion to IRVertex is needed.. otherwise the compiler complains :(
      }
      return converted;
    } else if (vertexToDelete instanceof MessageAggregatorVertex || vertexToDelete instanceof MessageBarrierVertex) {
      return messageVertexToGroup.get(vertexToDelete);
    } else {
      throw new IllegalArgumentException(vertexToDelete.getId());
    }
  }

  /**
   * Delete a group of vertex that corresponds to the specified vertex.
   * And then recursively delete neighboring utility vertices.
   *
   * (WARNING) Only call this method inside delete(), or inside this method itself.
   * This method uses buildWithoutSourceSinkCheck() for intermediate DAGs,
   * which will be finally checked in delete().
   *
   * @param vertexToDelete to delete
   * @param visited vertex groups (because cyclic dependencies between vertex groups are possible)
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
    if (vertexToDelete instanceof StreamVertex) {
      final DAGBuilder<IRVertex, IREdge> builder = rebuildExcluding(modifiedDAG, vertexGroupToDelete);

      // Add a new edge that directly connects the src of the stream vertex to its dst
      modifiedDAG.getOutgoingEdgesOf(vertexToDelete).stream()
        .filter(e -> !Util.isControlEdge(e))
        .map(IREdge::getDst)
        .forEach(dstVertex -> {
          modifiedDAG.getIncomingEdgesOf(vertexToDelete).stream()
            .filter(e -> !Util.isControlEdge(e))
            .map(IREdge::getSrc);
            /*
            .forEach(srcVertex-> { builder.connectVertices(
              Util.cloneEdge(streamVertexToOriginalEdge.get(vertexToDelete), srcVertex, dstVertex));
            });
            */
        });
      modifiedDAG = builder.buildWithoutSourceSinkCheck();
    } else if (vertexToDelete instanceof MessageAggregatorVertex || vertexToDelete instanceof MessageBarrierVertex) {
      modifiedDAG = rebuildExcluding(modifiedDAG, vertexGroupToDelete).buildWithoutSourceSinkCheck();
      final int deletedMessageId = vertexGroupToDelete.stream()
        .filter(vtd -> vtd instanceof MessageAggregatorVertex)
        .map(vtd -> ((MessageAggregatorVertex) vtd).getPropertyValue(MessageIdVertexProperty.class).get())
        .findAny().get();
      modifiedDAG.getEdges().stream()
        .filter(e -> e.getPropertyValue(MessageIdEdgeProperty.class).isPresent())
        .forEach(e -> e.getPropertyValue(MessageIdEdgeProperty.class).get().remove(deletedMessageId));
    } else if (vertexToDelete instanceof SamplingVertex) {
      modifiedDAG = rebuildExcluding(modifiedDAG, vertexGroupToDelete).buildWithoutSourceSinkCheck();
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


  public void insert(final StreamVertex streamVertex, final List<IREdge> edges) {
 final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
    // Insert the vertex.
    final IRVertex vertexToInsert = streamVertex;
    builder.addVertex(vertexToInsert);

    edges.forEach(edgeToStreamize -> {
      edgeToStreamize.getDst().getPropertyValue(ParallelismProperty.class)
        .ifPresent(p -> vertexToInsert.setProperty(ParallelismProperty.of(p)));
    });

    final AtomicBoolean fromSV1Added = new AtomicBoolean(false);

    // Build the new DAG to reflect the new topology.
    modifiedDAG.topologicalDo(v -> {
      builder.addVertex(v); // None of the existing vertices are deleted.

      for (final IREdge edge : modifiedDAG.getIncomingEdgesOf(v)) {
        if (edges.contains(edge)) {
          // MATCH!

          // Edge to the streamVertex
          final IREdge toSV1 = new IREdge(
            edge.getPropertyValue(CommunicationPatternProperty.class).get(),
            edge.getSrc(),
            vertexToInsert);
          edge.copyExecutionPropertiesTo(toSV1);

          // Edge from the streamVertex.
          final IREdge fromSV = new IREdge(CommunicationPatternProperty.Value.OneToOne, vertexToInsert, v);
          fromSV.setProperty(EncoderProperty.of(edge.getPropertyValue(EncoderProperty.class).get()));
          fromSV.setProperty(DecoderProperty.of(edge.getPropertyValue(DecoderProperty.class).get()));

          // fromSV.setProperty(EncoderProperty.of(edgeToStreamize.getPropertyValue(EncoderProperty.class).get()));
          // fromSV.setProperty(DecoderProperty.of(edgeToStreamize.getPropertyValue(DecoderProperty.class).get()));

          // Annotations for efficient data transfers - toSV
          // toSV1.setPropertyPermanently(DecoderProperty.of(BytesDecoderFactory.of()));
          // toSV.setPropertyPermanently(CompressionProperty.of(CompressionProperty.Value.LZ4));
          // toSV.setPropertyPermanently(DecompressionProperty.of(CompressionProperty.Value.None));

          // Annotations for efficient data transfers - fromSV
         //  fromSV.setPropertyPermanently(EncoderProperty.of(BytesEncoderFactory.of()));

          // fromSV.setPropertyPermanently(CompressionProperty.of(CompressionProperty.Value.None));
          // fromSV.setPropertyPermanently(DecompressionProperty.of(CompressionProperty.Value.LZ4));
          // fromSV.setPropertyPermanently(PartitionerProperty.of(PartitionerProperty.Type.DedicatedKeyPerElement));

          // Track the new edges.
          builder.connectVertices(toSV1);

          if (!fromSV1Added.get()) {
            builder.connectVertices(fromSV);
            fromSV1Added.set(true);
          }
        } else {
          // NO MATCH, so simply connect vertices as before.
          builder.connectVertices(edge);
        }
      }
    });


    for (final IREdge edgeToStreamize : edges) {
      // streamVertexToOriginalEdge.put(streamVertex, edgeToStreamize);
    }
    modifiedDAG = builder.build(); // update the DAG.
  }

  public void addTransientDataPath(
    final ConditionalRouterVertex vertexToStart,
    final List<IRVertex> verticesToAdd) {

    // duplicate verticesToAdd and add transient edge
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();

    // Add origin DAG because we do not change original vertices
    modifiedDAG.topologicalDo(v -> {
      builder.addVertex(v);
      for (final IREdge edge : modifiedDAG.getIncomingEdgesOf(v)) {
        builder.connectVertices(edge);
      }
    });

    // originVertex - newVertex mapping
    final Map<IRVertex, IRVertex> originToNewVertexMap = PassSharedData.originVertexToTransientVertexMap;

    LOG.info("Add newVertices for {}", verticesToAdd);

    // Add transient path vertices
    verticesToAdd.forEach(origin -> {
      if (!originToNewVertexMap.containsKey(origin)) {
        final IRVertex newVertex = new OperatorVertex((OperatorVertex) origin);
        origin.copyExecutionPropertiesTo(newVertex);

        builder.addVertex(newVertex);
        originToNewVertexMap.put(origin, newVertex);
      }
    });

    // Add transient data path edges
    for (final IRVertex addVertex : verticesToAdd) {
      final IRVertex newVertex = originToNewVertexMap.get(addVertex);

      final List<IREdge> inEdges = modifiedDAG.getIncomingEdgesOf(addVertex);
      final List<IREdge> outEdges = modifiedDAG.getOutgoingEdgesOf(addVertex);

      inEdges.forEach(edge -> {
        // Check whether this vertex is connected with the router vertex in source
        // If it is, we should add a transient edge to the router vertex and
        // connect with this vertex
        if (!originToNewVertexMap.containsKey(edge.getSrc())) {
          LOG.info("R2Reshaping Edge src {} dst {} transient path ", edge.getSrc(), newVertex);
          final IREdge newEdge = new IREdge(
            edge.getPropertyValue(CommunicationPatternProperty.class).get(),
            // this is conditional router vertex
            edge.getSrc(),
            newVertex);

          edge.copyExecutionPropertiesTo(newEdge);

          if (edge.getPropertyValue(CommunicationPatternProperty.class).get()
            .equals(CommunicationPatternProperty.Value.OneToOne)) {
            newEdge.setPropertyPermanently(
              CommunicationPatternProperty
                .of(CommunicationPatternProperty.Value.TransientOneToOne));
          } else if (edge.getPropertyValue(CommunicationPatternProperty.class).get()
            .equals(CommunicationPatternProperty.Value.RoundRobin)) {
             newEdge.setPropertyPermanently(
              CommunicationPatternProperty
                .of(CommunicationPatternProperty.Value.TransientRR));
          } else if (edge.getPropertyValue(CommunicationPatternProperty.class).get()
            .equals(CommunicationPatternProperty.Value.Shuffle)) {
            newEdge.setPropertyPermanently(
              CommunicationPatternProperty
                .of(CommunicationPatternProperty.Value.TransientShuffle));
          } else if (edge.getPropertyValue(CommunicationPatternProperty.class).get()
            .equals(CommunicationPatternProperty.Value.BroadCast)) {
            newEdge.setPropertyPermanently(
              CommunicationPatternProperty
                .of(CommunicationPatternProperty.Value.TransientBroadcast));
          }

          newEdge.setProperty(
            PairEdgeProperty.of(edge.getId()));

          // Add transient path for router vertex
          builder.connectVertices(newEdge);
        } else {
          final IREdge newEdge = new IREdge(
            edge.getPropertyValue(CommunicationPatternProperty.class).get(),
            originToNewVertexMap.get(edge.getSrc()),
            newVertex);
          edge.copyExecutionPropertiesTo(newEdge);

          if (!builder.hasEdge(newEdge)) {
            LOG.info("R2Reshaping Edge src {} dst {} normal path ", originToNewVertexMap.get(edge.getSrc()), newVertex);
            builder.connectVertices(newEdge);
          }
        }
      });

      outEdges.forEach(edge -> {
        // Check whether this vertex is sink or connected with the router vertex in the sink.
        // If it is, we should add a transient edge to the dst router vertex
        if (edge.getDst() instanceof ConditionalRouterVertex) {
          final IREdge newEdge = new IREdge(
            edge.getPropertyValue(CommunicationPatternProperty.class).get(),
            newVertex,
            // this is conditional router vertex
            edge.getDst());

          edge.copyExecutionPropertiesTo(newEdge);

          if (edge.getPropertyValue(CommunicationPatternProperty.class).get()
            .equals(CommunicationPatternProperty.Value.OneToOne)) {
            newEdge.setPropertyPermanently(
              CommunicationPatternProperty
                .of(CommunicationPatternProperty.Value.TransientOneToOne));

          } else if (edge.getPropertyValue(CommunicationPatternProperty.class).get()
            .equals(CommunicationPatternProperty.Value.Shuffle)) {
            newEdge.setPropertyPermanently(
              CommunicationPatternProperty
                .of(CommunicationPatternProperty.Value.TransientShuffle));
          } else if (edge.getPropertyValue(CommunicationPatternProperty.class).get()
            .equals(CommunicationPatternProperty.Value.BroadCast)) {
            newEdge.setPropertyPermanently(
              CommunicationPatternProperty
                .of(CommunicationPatternProperty.Value.TransientBroadcast));
          }

          newEdge.setProperty(
            PairEdgeProperty.of(edge.getId()));


          if (!builder.hasEdge(newEdge)) {
            // Add transient path for final edge
            // TODO: we should ignore watermarks when the transient path is not activated
            builder.connectVertices(newEdge);
          }
        }
      });
    }

    modifiedDAG = builder.build(); // update the DAG.
  }

  public void addStateMergerWithoutR2() {

    // Find StreamVertex -> GBK path
    // Change it to CR -> partial (lambda) -> final
    //                 -> partial (vm)     -> final


    // Create a completely new DAG with the vertex inserted.
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
    final List<IRVertex> gbks = new LinkedList<>();

    modifiedDAG.topologicalDo(vertex -> {
      if (vertex.isGBK) {
        if (!((OperatorVertex)vertex).getIsGlobalWindow()) {
          gbks.add(vertex);
        } else {
          LOG.info("Skipping partial agg for global window");
        }
      }
    });

    modifiedDAG.topologicalDo(vertex -> {
      if (!vertex.isGBK) {
        // Add origin vertex if it is not stateful
        LOG.info("Add vertex in R3 {}", vertex.getId());
        builder.addVertex(vertex);
        modifiedDAG.getIncomingEdgesOf(vertex).forEach(incomingEdge -> {
          if (!incomingEdge.getSrc().isGBK) {
            // Add edge if src is not stateful
            builder.connectVertices(incomingEdge);
          }
        });
      } else {
        if (((OperatorVertex)vertex).getIsGlobalWindow()) {
          LOG.info("Add global window vertex in R3 {}", vertex.getId());
          builder.addVertex(vertex);
          modifiedDAG.getIncomingEdgesOf(vertex).forEach(incomingEdge -> {
            if (!incomingEdge.getSrc().isGBK) {
              // Add edge if src is not stateful
              builder.connectVertices(incomingEdge);
            }
          });
        }
      }
    });

    // Find StreamVertex -> GBK path
    // Change it to CR -> partial (lambda) -> final
    //                 -> partial (vm)     -> final

    for (int i = 0; i < gbks.size(); i++) {
      final IRVertex originGBK = gbks.get(i);
      final IRVertex transientGBK = new OperatorVertex((OperatorVertex) originGBK);
      originGBK.copyExecutionPropertiesTo(transientGBK);

      final OperatorVertex partialOrigin = ((OperatorVertex) originGBK).getPartialCombine();
      try {
        originGBK.getPropertyValue(ParallelismProperty.class)
          .ifPresent(p -> partialOrigin.setProperty(ParallelismProperty.of(p)));
      } catch (final Exception e) {
        e.printStackTrace();
        throw new RuntimeException("OriginGBK: " + originGBK.getId() + ", " +
          ((OperatorVertex) originGBK).getTransform() + ", " + ((OperatorVertex) originGBK).getPartialCombine());
      }

      // Get encoder and transform
      // Optimization of R3 !!
      final EncoderFactory ef =
        modifiedDAG.getOutgoingEdgesOf(originGBK).get(0).getPropertyValue(EncoderProperty.class).get();
      partialOrigin.setOriginEncoderFactory(ef);

      LOG.info("Set final transform to partial origin {}", partialOrigin.getId());
      partialOrigin.isGBK = true;

      final OperatorVertex partialTransient =
        new OperatorVertex(((OperatorVertex)originGBK).getPartialCombine());
      originGBK.getPropertyValue(ParallelismProperty.class)
        .ifPresent(p -> partialTransient.setProperty(ParallelismProperty.of(p)));

      partialTransient.isGBK = true;

      // Get encoder and transform
      // Optimization of R3 !!
      partialTransient.setOriginEncoderFactory(ef);
      LOG.info("Set final transform to partial transient {}", partialTransient.getId());

      // State merger
      final StateMergerVertex stateMerger = new StateMergerVertex(((OperatorVertex)originGBK).getFinalCombine());
      originGBK.getPropertyValue(ParallelismProperty.class)
        .ifPresent(p -> stateMerger.setProperty(ParallelismProperty.of(p)));

      stateMerger.isGBK = true;

      // Add partial origin and transient vertex
      builder.addVertex(partialOrigin);
      builder.addVertex(partialTransient);
      // Add merger
      builder.addVertex(stateMerger);

      final Map<String, String> oldNewEdgeId = new HashMap<>();

      // connect edge for partial origin
      modifiedDAG.getIncomingEdgesOf(originGBK).forEach(edge -> {
        // Add CR -> partial origin
        final IREdge toPartialOriginEdge = new IREdge(
          edge.getPropertyValue(CommunicationPatternProperty.class).get(),
          edge.getSrc(),
          partialOrigin);

        oldNewEdgeId.put(edge.getId(), toPartialOriginEdge.getId());
        edge.copyExecutionPropertiesTo(toPartialOriginEdge);
        builder.connectVertices(toPartialOriginEdge);

        // Add CR -> partial transient
        final IREdge toPartialTransientEdge = new IREdge(
          edge.getPropertyValue(CommunicationPatternProperty.class).get(),
          edge.getSrc(),
          partialTransient);
        edge.copyExecutionPropertiesTo(toPartialTransientEdge);

        if (edge.getPropertyValue(CommunicationPatternProperty.class).get()
          .equals(CommunicationPatternProperty.Value.OneToOne)) {
          toPartialTransientEdge.setPropertyPermanently(
            CommunicationPatternProperty
              .of(CommunicationPatternProperty.Value.TransientOneToOne));
        } else if (edge.getPropertyValue(CommunicationPatternProperty.class).get()
          .equals(CommunicationPatternProperty.Value.RoundRobin)) {
          toPartialTransientEdge.setPropertyPermanently(
            CommunicationPatternProperty
              .of(CommunicationPatternProperty.Value.TransientRR));
        } else if (edge.getPropertyValue(CommunicationPatternProperty.class).get()
          .equals(CommunicationPatternProperty.Value.Shuffle)) {
          toPartialTransientEdge.setPropertyPermanently(
            CommunicationPatternProperty
              .of(CommunicationPatternProperty.Value.TransientShuffle));
        } else if (edge.getPropertyValue(CommunicationPatternProperty.class).get()
          .equals(CommunicationPatternProperty.Value.BroadCast)) {
          toPartialTransientEdge.setPropertyPermanently(
            CommunicationPatternProperty
              .of(CommunicationPatternProperty.Value.TransientBroadcast));
        }

        toPartialTransientEdge.setProperty(
          PairEdgeProperty.of(toPartialOriginEdge.getId()));

        // final String newPairEdgeId =
        //  oldNewEdgeId.get(edge.getPropertyValue(PairEdgeProperty.class).get());

        LOG.info("Old new edge id: {} " +
            "transient edge {}", oldNewEdgeId, toPartialTransientEdge.getId());

        //toPartialTransientEdge.setProperty(PairEdgeProperty.of(newPairEdgeId));
        builder.connectVertices(toPartialTransientEdge);
      });

      // connect edge partial origin -> merger
      final IREdge fromPartialOriginToMerger = new IREdge(
        CommunicationPatternProperty.Value.OneToOne,
        partialOrigin,
        stateMerger);
      final IREdge pToFinalEdge = ((OperatorVertex) originGBK).getPartialToFinalEdge();
      pToFinalEdge.copyExecutionPropertiesTo(fromPartialOriginToMerger);

      fromPartialOriginToMerger.setPropertyPermanently(
        CommunicationPatternProperty.of(CommunicationPatternProperty.Value.OneToOne));

      builder.connectVertices(fromPartialOriginToMerger);

      // connect edge partial transient -> merger
      final IREdge fromPartialTransientToMerger = new IREdge(
        CommunicationPatternProperty.Value.TransientOneToOne,
        partialTransient,
        stateMerger);
      pToFinalEdge.copyExecutionPropertiesTo(fromPartialTransientToMerger);

      fromPartialTransientToMerger.setPropertyPermanently(
        CommunicationPatternProperty.of(CommunicationPatternProperty.Value.TransientOneToOne));

      fromPartialTransientToMerger.setProperty(PairEdgeProperty.of(fromPartialOriginToMerger.getId()));

      builder.connectVertices(fromPartialTransientToMerger);

      // connect edge merger -> originGBK_out
      final IREdge originGBKOutEdge = modifiedDAG.getOutgoingEdgesOf(originGBK).get(0);

      final IREdge mergerToOriginGBKOut = new IREdge(
        originGBKOutEdge.getPropertyValue(CommunicationPatternProperty.class).get(),
        stateMerger,
        originGBKOutEdge.getDst());
      originGBKOutEdge.copyExecutionPropertiesTo(mergerToOriginGBKOut);

      builder.connectVertices(mergerToOriginGBKOut);
    }

    modifiedDAG = builder.build(); // update the DAG.
  }

  public void addStateMerger() {
    // Create a completely new DAG with the vertex inserted.
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();

    // Find gbk finals
    // We should change its vertex to partial -> final combine
    final List<IRVertex> gbks = PassSharedData.originVertexToTransientVertexMap.keySet()
      .stream().filter(vertex -> vertex.isGBK).collect(Collectors.toList());

    final List<IRVertex> gbkTransientPaths = gbks.stream()
      .map(gbk -> PassSharedData.originVertexToTransientVertexMap.get(gbk)).collect(Collectors.toList());

    modifiedDAG.topologicalDo(vertex -> {
      if (!vertex.isGBK) {
        // Add origin vertex if it is not stateful
        LOG.info("Add vertex in R3 {}", vertex.getId());
        builder.addVertex(vertex);
        modifiedDAG.getIncomingEdgesOf(vertex).forEach(incomingEdge -> {
          if (!incomingEdge.getSrc().isGBK) {
            // Add edge if src is not stateful
            builder.connectVertices(incomingEdge);
          }
        });
      }
    });

    for (int i = 0; i < gbks.size(); i++) {
      final IRVertex originGBK = gbks.get(i);
      final IRVertex transientGBK = gbkTransientPaths.get(i);

      final OperatorVertex partialOrigin = ((OperatorVertex)originGBK).getPartialCombine();
      originGBK.getPropertyValue(ParallelismProperty.class)
        .ifPresent(p -> partialOrigin.setProperty(ParallelismProperty.of(p)));

      // Get encoder and transform
      // Optimization of R3 !!
      final EncoderFactory ef =
        modifiedDAG.getOutgoingEdgesOf(originGBK).get(0).getPropertyValue(EncoderProperty.class).get();
      partialOrigin.setOriginEncoderFactory(ef);

      LOG.info("Set final transform to partial origin {}", partialOrigin.getId());
      partialOrigin.isGBK = true;

      final OperatorVertex partialTransient =
        new OperatorVertex(((OperatorVertex)originGBK).getPartialCombine());
      originGBK.getPropertyValue(ParallelismProperty.class)
        .ifPresent(p -> partialTransient.setProperty(ParallelismProperty.of(p)));

      partialTransient.isGBK = true;

      // Get encoder and transform
      // Optimization of R3 !!
      partialTransient.setOriginEncoderFactory(ef);
      LOG.info("Set final transform to partial transient {}", partialTransient.getId());

      // State merger
      final StateMergerVertex stateMerger = new StateMergerVertex(((OperatorVertex)originGBK).getFinalCombine());
      originGBK.getPropertyValue(ParallelismProperty.class)
        .ifPresent(p -> stateMerger.setProperty(ParallelismProperty.of(p)));

      stateMerger.isGBK = true;

      // Add partial origin and transient vertex
      builder.addVertex(partialOrigin);
      builder.addVertex(partialTransient);
      // Add merger
      builder.addVertex(stateMerger);

      final Map<String, String> oldNewEdgeId = new HashMap<>();

      // connect edge for partial origin
      modifiedDAG.getIncomingEdgesOf(originGBK).forEach(edge -> {
        final IREdge toPartialOriginEdge = new IREdge(
          edge.getPropertyValue(CommunicationPatternProperty.class).get(),
          edge.getSrc(),
          partialOrigin);

        oldNewEdgeId.put(edge.getId(), toPartialOriginEdge.getId());
        edge.copyExecutionPropertiesTo(toPartialOriginEdge);
        builder.connectVertices(toPartialOriginEdge);
      });

      // connect edge for partial transient
      modifiedDAG.getIncomingEdgesOf(transientGBK).forEach(edge -> {
        final IREdge toPartialTransientEdge = new IREdge(
          edge.getPropertyValue(CommunicationPatternProperty.class).get(),
          edge.getSrc(),
          partialTransient);
        edge.copyExecutionPropertiesTo(toPartialTransientEdge);

        // final String newPairEdgeId =
        //  oldNewEdgeId.get(edge.getPropertyValue(PairEdgeProperty.class).get());

        LOG.info("Old new edge id: {} " +
          "transient edge {} old pair: {}", oldNewEdgeId, toPartialTransientEdge.getId(),
          edge.getPropertyValue(PairEdgeProperty.class));

        //toPartialTransientEdge.setProperty(PairEdgeProperty.of(newPairEdgeId));
        builder.connectVertices(toPartialTransientEdge);
      });

      // connect edge partial origin -> merger
       final IREdge fromPartialOriginToMerger = new IREdge(
         CommunicationPatternProperty.Value.OneToOne,
          partialOrigin,
          stateMerger);
       final IREdge pToFinalEdge = ((OperatorVertex) originGBK).getPartialToFinalEdge();
       pToFinalEdge.copyExecutionPropertiesTo(fromPartialOriginToMerger);

       fromPartialOriginToMerger.setPropertyPermanently(
              CommunicationPatternProperty.of(CommunicationPatternProperty.Value.OneToOne));

       builder.connectVertices(fromPartialOriginToMerger);

      // connect edge partial transient -> merger
      final IREdge fromPartialTransientToMerger = new IREdge(
        CommunicationPatternProperty.Value.TransientOneToOne,
        partialTransient,
        stateMerger);
      pToFinalEdge.copyExecutionPropertiesTo(fromPartialTransientToMerger);

      fromPartialTransientToMerger.setPropertyPermanently(
        CommunicationPatternProperty.of(CommunicationPatternProperty.Value.TransientOneToOne));

      fromPartialTransientToMerger.setProperty(PairEdgeProperty.of(fromPartialOriginToMerger.getId()));

      builder.connectVertices(fromPartialTransientToMerger);

      // connect edge merger -> originGBK_out
      //                     -> transientGBK_out
      final IREdge originGBKOutEdge = modifiedDAG.getOutgoingEdgesOf(originGBK).get(0);
      final IREdge transientGBKOutEdge = modifiedDAG.getOutgoingEdgesOf(transientGBK).get(0);

      final IREdge mergerToOriginGBKOut = new IREdge(
        originGBKOutEdge.getPropertyValue(CommunicationPatternProperty.class).get(),
        stateMerger,
        originGBKOutEdge.getDst());
      originGBKOutEdge.copyExecutionPropertiesTo(mergerToOriginGBKOut);

      builder.connectVertices(mergerToOriginGBKOut);

      if (transientGBKOutEdge.getPropertyValue(CommunicationPatternProperty.class).get()
        .equals(CommunicationPatternProperty.Value.OneToOne)) {
        final IREdge mergerToTransientGBKOut = new IREdge(
          CommunicationPatternProperty.Value.TransientOneToOne,
          stateMerger,
          transientGBKOutEdge.getDst());
        transientGBKOutEdge.copyExecutionPropertiesTo(mergerToTransientGBKOut);
        mergerToTransientGBKOut.setPropertyPermanently(
          CommunicationPatternProperty.of(CommunicationPatternProperty.Value.TransientOneToOne));

        mergerToTransientGBKOut.setProperty(
          PairEdgeProperty.of(mergerToOriginGBKOut.getId()));

        builder.connectVertices(mergerToTransientGBKOut);
      } else {
        final IREdge mergerToTransientGBKOut = new IREdge(
          CommunicationPatternProperty.Value.TransientShuffle,
          stateMerger,
          transientGBKOutEdge.getDst());
        transientGBKOutEdge.copyExecutionPropertiesTo(mergerToTransientGBKOut);
        mergerToTransientGBKOut.setPropertyPermanently(
          CommunicationPatternProperty.of(CommunicationPatternProperty.Value.TransientShuffle));

        mergerToTransientGBKOut.setProperty(
          PairEdgeProperty.of(mergerToOriginGBKOut.getId()));

        builder.connectVertices(mergerToTransientGBKOut);
      }

    }

    modifiedDAG = builder.build(); // update the DAG.
  }


  public void insertConditionalRouter(final IREdge edgeToAdd,
                                      final List<IREdge> toFinalEdges) {

    // Create a completely new DAG with the vertex inserted.
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();

    // Integrity check
    if (edgeToAdd.getPropertyValue(MessageIdEdgeProperty.class).isPresent()
      && !edgeToAdd.getPropertyValue(MessageIdEdgeProperty.class).get().isEmpty()) {
      throw new CompileTimeOptimizationException(edgeToAdd.getId() + " has a MessageId, and cannot be removed");
    }

    /*
    if (!edgeToAdd.getPropertyValue(CommunicationPatternProperty.class).get()
      .equals(CommunicationPatternProperty.Value.Shuffle)) {
      throw new RuntimeException("Conditional router output is not shuffle..." + edgeToAdd);
    }
    */

    // Before: A -edgeToAdd-> B (partial) -> C (final)
    // After: A -(1)-> ConditionalRouter -(2)-> B (partial) -> C (final)
    //                               -----(3)----> B' (pc)   (4) -/
    // (1), (2) edge 새로 추가.
    // CR, B' 추가

    // Insert the vertex.
    final IRVertex vertexToInsert = new ConditionalRouterVertex(new CRTransform());
    final IRVertex partialCombine = new OperatorVertex(
      ((OperatorVertex)edgeToAdd.getDst()).getTransform());
    partialCombine.isGBK = true;

    builder.addVertex(vertexToInsert);
    builder.addVertex(partialCombine);

    edgeToAdd.getDst().getPropertyValue(ParallelismProperty.class)
      .ifPresent(p -> vertexToInsert.setProperty(ParallelismProperty.of(p)));

    edgeToAdd.getDst().getPropertyValue(ParallelismProperty.class)
      .ifPresent(p -> partialCombine.setProperty(ParallelismProperty.of(p)));

    // Build the new DAG to reflect the new topology.
    modifiedDAG.topologicalDo(v -> {
      builder.addVertex(v); // None of the existing vertices are deleted.

      for (final IREdge edge : modifiedDAG.getIncomingEdgesOf(v)) {
        if (edge.equals(edgeToAdd)) {
          // MATCH!

          // Edge to the streamVertex
          final IREdge toSV = new IREdge(CommunicationPatternProperty.Value.OneToOne,
            edgeToAdd.getSrc(), vertexToInsert);

          // Edge from the streamVertex.
          final IREdge fromSVRR = new IREdge(
            edgeToAdd.getPropertyValue(CommunicationPatternProperty.class).get(),
            vertexToInsert, partialCombine);
          edgeToAdd.copyExecutionPropertiesTo(fromSVRR);

          if (edgeToAdd.getPropertyValue(CommunicationPatternProperty.class).get()
            .equals(CommunicationPatternProperty.Value.OneToOne)) {
            fromSVRR.setPropertyPermanently(
              CommunicationPatternProperty.of(CommunicationPatternProperty.Value.TransientOneToOne));
          }


          final IREdge fromSVShuffle = new IREdge(
            edgeToAdd.getPropertyValue(CommunicationPatternProperty.class).get(),
            vertexToInsert,
            edgeToAdd.getDst());
          edgeToAdd.copyExecutionPropertiesTo(fromSVShuffle);

          if (edgeToAdd.getPropertyValue(CommunicationPatternProperty.class).get()
            .equals(CommunicationPatternProperty.Value.OneToOne)) {
            fromSVShuffle.setPropertyPermanently(
              CommunicationPatternProperty.of(CommunicationPatternProperty.Value.TransientOneToOne));
          }

          // Track the new edges.
          LOG.info("Add edge toSV {}", toSV.getId());
          builder.connectVertices(toSV);
          LOG.info("Add edge fromSVRR {}", fromSVRR.getId());
          builder.connectVertices(fromSVRR);
          LOG.info("Add edge fromSVShuffle {}", fromSVShuffle.getId());
          builder.connectVertices(fromSVShuffle);

        } else if (toFinalEdges.contains(edge)) {
          // partial' -> final
          final IREdge toFinalEdge = edge;
          final IREdge pToFinal = new IREdge(
            edge.getPropertyValue(CommunicationPatternProperty.class).get(),
            partialCombine,
            toFinalEdge.getDst());
          toFinalEdge.copyExecutionPropertiesTo(pToFinal);

          // pToFinal.setPropertyPermanently(
          //  CommunicationPatternProperty.of(CommunicationPatternProperty.Value.Shuffle));

          builder.connectVertices(toFinalEdge);
          LOG.info("Final edge {}", toFinalEdge.getId());

          builder.connectVertices(pToFinal);
          LOG.info("Add edge pToFinal {}", pToFinal.getId());
        } else {
          // NO MATCH, so simply connect vertices as before.
          builder.connectVertices(edge);
        }
      }
    });

    modifiedDAG = builder.build(); // update the DAG.
  }

  private boolean isFlatten(final IRVertex v) {
    if (v instanceof OperatorVertex) {
      return ((OperatorVertex)v).getTransform().isFlatten();
    } else {
      return false;
    }
  }

  public void removeFlatten() {
    // Create a completely new DAG with the vertex inserted.
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();

    // Build the new DAG to reflect the new topology.
    modifiedDAG.topologicalDo(v -> {
      if (!isFlatten(v)) {
        LOG.info("Add vertex for rm flatten: {}", v.getId());
        builder.addVertex(v); // None of the existing vertices are deleted.
      }
    });


    // add edge
    modifiedDAG.topologicalDo(v -> {
      for (final IREdge edge : modifiedDAG.getIncomingEdgesOf(v)) {
        if (isFlatten(edge.getDst())) {
          final IRVertex flatten = edge.getDst();
          modifiedDAG.getOutgoingEdgesOf(flatten).forEach(outEdge -> {
            final IREdge newEdge = new IREdge(
              edge.getPropertyValue(CommunicationPatternProperty.class).get(),
              edge.getSrc(),
              outEdge.getDst());
            edge.copyExecutionPropertiesTo(newEdge);
            builder.connectVertices(newEdge);
          });
        } else if (isFlatten(edge.getSrc())) {
          // skip
        } else {
          builder.connectVertices(edge);
        }
      }
    });

    modifiedDAG = builder.build(); // update the DAG.
  }

  public void change(final OperatorVertex origin, final OperatorVertex dst) {

    // Create a completely new DAG with the vertex inserted.
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();

    // Insert the vertex.
    builder.addVertex(dst);
    origin.copyExecutionPropertiesTo(dst);
    origin.getPropertyValue(ParallelismProperty.class)
      .ifPresent(p -> dst.setProperty(ParallelismProperty.of(p)));

    // Build the new DAG to reflect the new topology.
    modifiedDAG.topologicalDo(v -> {

      if (!v.equals(origin)) {
        builder.addVertex(v); // None of the existing vertices are deleted.
      }

      for (final IREdge edge : modifiedDAG.getIncomingEdgesOf(v)) {
        if (edge.getDst().equals(origin)) {
          final IREdge newEdge = new IREdge(
            edge.getPropertyValue(CommunicationPatternProperty.class).get(),
            edge.getSrc(),
            dst);
          edge.copyExecutionPropertiesTo(newEdge);

          builder.connectVertices(newEdge);
        } else if (edge.getSrc().equals(origin)) {
          final IREdge newEdge = new IREdge(
            edge.getPropertyValue(CommunicationPatternProperty.class).get(),
            dst,
            edge.getDst());
          edge.copyExecutionPropertiesTo(newEdge);

          builder.connectVertices(newEdge);
        } else {
          builder.connectVertices(edge);
        }
      }
    });

    modifiedDAG = builder.build(); // update the DAG.
  }

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
  public void insert(final OperatorVertex streamVertex, final IREdge edgeToStreamize) {
    assertNonExistence(streamVertex);
    assertNonControlEdge(edgeToStreamize);

    // Create a completely new DAG with the vertex inserted.
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();

    // Integrity check
    if (edgeToStreamize.getPropertyValue(MessageIdEdgeProperty.class).isPresent()
      && !edgeToStreamize.getPropertyValue(MessageIdEdgeProperty.class).get().isEmpty()) {
      throw new CompileTimeOptimizationException(edgeToStreamize.getId() + " has a MessageId, and cannot be removed");
    }

    // Insert the vertex.
    final IRVertex vertexToInsert = wrapSamplingVertexIfNeeded(streamVertex, edgeToStreamize.getSrc());
    builder.addVertex(vertexToInsert);
    edgeToStreamize.getDst().getPropertyValue(ParallelismProperty.class)
      .ifPresent(p -> vertexToInsert.setProperty(ParallelismProperty.of(p)));

    // Build the new DAG to reflect the new topology.
    modifiedDAG.topologicalDo(v -> {
      builder.addVertex(v); // None of the existing vertices are deleted.

      for (final IREdge edge : modifiedDAG.getIncomingEdgesOf(v)) {
        if (edge.equals(edgeToStreamize)) {
          // MATCH!

          // Edge to the streamVertex
          final IREdge toSV;
          final IREdge fromSV;
          if (streamVertex instanceof SrcStreamVertex) {
            toSV = new IREdge(
                CommunicationPatternProperty.Value.OneToOne,
              edgeToStreamize.getSrc(),
              vertexToInsert);
            // edgeToStreamize.copyExecutionPropertiesTo(toSV);

            toSV.setPropertyPermanently(EncoderProperty.of(edgeToStreamize
              .getPropertyValue(EncoderProperty.class).get()));

            fromSV = new IREdge(
              edgeToStreamize.getPropertyValue(CommunicationPatternProperty.class).get(),
              vertexToInsert,
              v);
            edgeToStreamize.copyExecutionPropertiesTo(fromSV);

          } else {
            toSV = new IREdge(
              edgeToStreamize.getPropertyValue(CommunicationPatternProperty.class).get(),
              edgeToStreamize.getSrc(),
              vertexToInsert);
            edgeToStreamize.copyExecutionPropertiesTo(toSV);

            // Edge from the streamVertex.
            fromSV = new IREdge(CommunicationPatternProperty.Value.OneToOne, vertexToInsert, v);
            fromSV.setProperty(EncoderProperty.of(edgeToStreamize.getPropertyValue(EncoderProperty.class).get()));
            fromSV.setProperty(DecoderProperty.of(edgeToStreamize.getPropertyValue(DecoderProperty.class).get()));
          }

          // Annotations for efficient data transfers - toSV
          // toSV.setPropertyPermanently(DecoderProperty.of(BytesDecoderFactory.of()));

          // Annotations for efficient data transfers - fromSV
          // fromSV.setPropertyPermanently(EncoderProperty.of(BytesEncoderFactory.of()));

          // fromSV.setPropertyPermanently(CompressionProperty.of(CompressionProperty.Value.None));
          // fromSV.setPropertyPermanently(DecompressionProperty.of(CompressionProperty.Value.LZ4));
          // fromSV.setPropertyPermanently(PartitionerProperty.of(PartitionerProperty.Type.DedicatedKeyPerElement));

          // Track the new edges.
          builder.connectVertices(toSV);
          builder.connectVertices(fromSV);
        } else {
          // NO MATCH, so simply connect vertices as before.
          builder.connectVertices(edge);
        }
      }
    });

    /*
    if (edgeToStreamize.getSrc() instanceof StreamVertex) {
      streamVertexToOriginalEdge.put(streamVertex, streamVertexToOriginalEdge.get(edgeToStreamize.getSrc()));
    } else if (edgeToStreamize.getDst() instanceof StreamVertex) {
      streamVertexToOriginalEdge.put(streamVertex, streamVertexToOriginalEdge.get(edgeToStreamize.getDst()));
    } else {
      streamVertexToOriginalEdge.put(streamVertex, edgeToStreamize);
    }
    */
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
   * TODO #345: Simplify insert(MessageBarrierVertex)
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
    assertNonExistence(messageBarrierVertex);
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

    ////////////////////////////////// STEP 1: Insert new vertices and edges (src - mbv - mav - dst)

    // From src to mbv
    final List<IRVertex> mbvList = new ArrayList<>();
    for (final IREdge edge : edgesToGetStatisticsOf) {
      final IRVertex mbvToAdd = wrapSamplingVertexIfNeeded(
        new MessageBarrierVertex<>(messageBarrierVertex.getMessageFunction()), edge.getSrc());
      builder.addVertex(mbvToAdd);
      mbvList.add(mbvToAdd);
      edge.getSrc().getPropertyValue(ParallelismProperty.class)
        .ifPresent(p -> mbvToAdd.setProperty(ParallelismProperty.of(p)));

      final IREdge edgeToClone = edge;
      /*
      if (edge.getSrc() instanceof StreamVertex) {
        edgeToClone = streamVertexToOriginalEdge.get(edge.getSrc());
      } else if (edge.getDst() instanceof StreamVertex) {
        edgeToClone = streamVertexToOriginalEdge.get(edge.getDst());
      } else {
        edgeToClone = edge;
      }
      */

      final IREdge clone = Util.cloneEdge(
        CommunicationPatternProperty.Value.OneToOne, edgeToClone, edge.getSrc(), mbvToAdd);
      builder.connectVertices(clone);
    }

    // Add mav (no need to wrap inside sampling vertices)
    builder.addVertex(messageAggregatorVertex);

    // From mbv to mav
    for (final IRVertex mbv : mbvList) {
      final IREdge edgeToMav = edgeToMessageAggregator(
        mbv, messageAggregatorVertex, mbvOutputEncoder, mbvOutputDecoder);
      builder.connectVertices(edgeToMav);
    }

    // From mav to dst
    // Add a control dependency (no output) from the messageAggregatorVertex to the destination.
    builder.connectVertices(
      Util.createControlEdge(messageAggregatorVertex, edgesToGetStatisticsOf.iterator().next().getDst()));

    ////////////////////////////////// STEP 2: Annotate the MessageId on optimization target edges

    modifiedDAG.topologicalDo(v -> {
      modifiedDAG.getIncomingEdgesOf(v).forEach(inEdge -> {
        if (edgesToOptimize.contains(inEdge)) {
          final HashSet<Integer> msgEdgeIds =
            inEdge.getPropertyValue(MessageIdEdgeProperty.class).orElse(new HashSet<>(0));
          msgEdgeIds.add(messageAggregatorVertex.getPropertyValue(MessageIdVertexProperty.class).get());
          inEdge.setProperty(MessageIdEdgeProperty.of(msgEdgeIds));
        }
      });
    });

    final Set<IRVertex> insertedVertices = new HashSet<>();
    insertedVertices.addAll(mbvList);
    insertedVertices.add(messageAggregatorVertex);
    mbvList.forEach(mbv -> messageVertexToGroup.put(mbv, insertedVertices));
    messageVertexToGroup.put(messageAggregatorVertex, insertedVertices);

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
   * (Future calls to insert() can add new vertices that connect to sampling vertices. Such new vertices will also be
   * wrapped with sampling vertices, as new vertices that consume outputs from sampling vertices will process
   * a subset of data anyways, and no such new vertex will reach the original DAG except via control edges)
   *
   * TODO #343: Extend SamplingVertex control edges
   *
   * @param toInsert sampling vertices.
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
   * Reshape unsafely, without guarantees on preserving application semantics.
   * TODO #330: Refactor Unsafe Reshaping Passes
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
   * @param mbv src.
   * @param mav dst.
   * @param encoder src-dst encoder.
   * @param decoder src-dst decoder.
   * @return the edge.
   */
  private IREdge edgeToMessageAggregator(final IRVertex mbv,
                                         final IRVertex mav,
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
    newEdge.setPropertyPermanently(encoder);
    newEdge.setPropertyPermanently(decoder);
    newEdge.setPropertyPermanently(KeyExtractorProperty.of(pairKeyExtractor));

    // TODO #345: Simplify insert(MessageBarrierVertex)
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

  @Override
  public String toString() {
    return asJsonNode().toString();
  }
}
