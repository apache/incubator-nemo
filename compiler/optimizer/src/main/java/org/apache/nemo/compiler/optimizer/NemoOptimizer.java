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
package org.apache.nemo.compiler.optimizer;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.eventhandler.PubSubEventHandlerWrapper;
import org.apache.nemo.common.exception.CompileTimeOptimizationException;
import org.apache.nemo.common.exception.DynamicOptimizationException;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.CachedSourceVertex;
import org.apache.nemo.common.ir.edge.executionproperty.CacheIDProperty;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.IgnoreSchedulingTempDataReceiverProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.compiler.optimizer.policy.Policy;
import org.apache.nemo.conf.JobConf;
import net.jcip.annotations.NotThreadSafe;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;

/**
 * An interface for optimizer, which manages the optimization over submitted IR DAGs through {@link Policy}s.
 * The instance of this class will reside in driver.
 */
@NotThreadSafe
public final class NemoOptimizer implements Optimizer {

  private final String dagDirectory;
  private final String optimizationPolicyCanonicalName;
  private final Injector injector;
  private final PubSubEventHandlerWrapper pubSubWrapper;
  private final Map<UUID, Integer> cacheIdToParallelism = new HashMap<>();
  private int irDagCount = 0;

  /**
   * @param dagDirectory to store JSON representation of intermediate DAGs.
   * @param optimizationPolicy the name of the optimization policy.
   * @param pubSubEventHandlerWrapper pub/sub event handler.
   * @param injector reef injector.
   */
  @Inject
  private NemoOptimizer(@Parameter(JobConf.DAGDirectory.class) final String dagDirectory,
                        @Parameter(JobConf.OptimizationPolicy.class) final String optimizationPolicy,
                        final PubSubEventHandlerWrapper pubSubEventHandlerWrapper,
                        final Injector injector) {
    this.dagDirectory = dagDirectory;
    this.optimizationPolicyCanonicalName = optimizationPolicy;
    this.injector = injector;
    this.pubSubWrapper = pubSubEventHandlerWrapper;
  }

  /**
   * Optimize the submitted DAG.
   *
   * @param dag the input DAG to optimize.
   * @return optimized DAG, reshaped or tagged with execution properties.
   */
  @Override
  public DAG<IRVertex, IREdge> optimizeDag(final DAG<IRVertex, IREdge> dag) {
    final String irDagId = "ir-" + irDagCount++ + "-";
    dag.storeJSON(dagDirectory, irDagId, "IR before optimization");

    final DAG<IRVertex, IREdge> optimizedDAG;
    final Policy optimizationPolicy;
    final Map<UUID, IREdge> cacheIdToEdge = new HashMap<>();

    try {
      // Handle caching first.
      final DAG<IRVertex, IREdge> cacheFilteredDag = handleCaching(dag, cacheIdToEdge);
      if (!cacheIdToEdge.isEmpty()) {
        cacheFilteredDag.storeJSON(dagDirectory, irDagId + "FilterCache",
            "IR after cache filtering");
      }

      // Conduct compile-time optimization.
      optimizationPolicy = (Policy) Class.forName(optimizationPolicyCanonicalName).newInstance();

      if (optimizationPolicy == null) {
        throw new CompileTimeOptimizationException("A policy name should be specified.");
      }

      optimizedDAG = optimizationPolicy.runCompileTimeOptimization(cacheFilteredDag, dagDirectory);
      optimizedDAG.storeJSON(dagDirectory, irDagId + optimizationPolicy.getClass().getSimpleName(),
          "IR optimized for " + optimizationPolicy.getClass().getSimpleName());
    } catch (final Exception e) {
      throw new CompileTimeOptimizationException(e);
    }

    // Register run-time optimization.
    try {
      optimizationPolicy.registerRunTimeOptimizations(injector, pubSubWrapper);
    } catch (final Exception e) {
      throw new DynamicOptimizationException(e);
    }

    // Update cached list.
    // TODO #191: Report the actual state of cached data to optimizer.
    // Now we assume that the optimized dag always run properly.
    cacheIdToEdge.forEach((cacheId, edge) -> {
      if (!cacheIdToParallelism.containsKey(cacheId)) {
        cacheIdToParallelism.put(
            cacheId, optimizedDAG.getVertexById(edge.getDst().getId()).getPropertyValue(ParallelismProperty.class)
                .orElseThrow(() -> new RuntimeException("No parallelism on an IR vertex.")));
      }
    });

    // Return optimized dag
    return optimizedDAG;
  }

  /**
   * Handle data caching.
   * At first, it search the edges having cache ID from the given dag and update them to the given map.
   * Then, if some edge of a submitted dag is annotated as "cached" and the data was produced already,
   * the part of the submitted dag which produces the cached data will be cropped and the last vertex
   * before the cached edge will be replaced with a cached data source vertex.
   * This cached edge will be detected and appended to the original dag in scheduler.
   *
   * @param dag           the dag to handle.
   * @param cacheIdToEdge the map from cache ID to edge to update.
   * @return the cropped dag regarding to caching.
   */
  private DAG<IRVertex, IREdge> handleCaching(final DAG<IRVertex, IREdge> dag,
                                              final Map<UUID, IREdge> cacheIdToEdge) {
    dag.topologicalDo(irVertex ->
        dag.getIncomingEdgesOf(irVertex).forEach(
            edge -> edge.getPropertyValue(CacheIDProperty.class).
                ifPresent(cacheId -> cacheIdToEdge.put(cacheId, edge))
        ));

    if (cacheIdToEdge.isEmpty()) {
      return dag;
    } else {
      final DAGBuilder<IRVertex, IREdge> filteredDagBuilder = new DAGBuilder<>();
      final List<IRVertex> sinkVertices = dag.getVertices().stream()
          .filter(irVertex -> dag.getOutgoingEdgesOf(irVertex).isEmpty())
          .collect(Collectors.toList());
      sinkVertices.forEach(filteredDagBuilder::addVertex); // Sink vertex cannot be cached already.

      sinkVertices.forEach(sinkVtx -> addNonCachedVerticesAndEdges(dag, sinkVtx, filteredDagBuilder));

      return filteredDagBuilder.buildWithoutSourceCheck();
    }
  }

  /**
   * Recursively add vertices and edges after cached edges to the dag builder in reversed order.
   *
   * @param dag      the original dag to filter.
   * @param irVertex the ir vertex to consider to add.
   * @param builder  the filtered dag builder.
   */
  private void addNonCachedVerticesAndEdges(final DAG<IRVertex, IREdge> dag,
                                            final IRVertex irVertex,
                                            final DAGBuilder<IRVertex, IREdge> builder) {
    if (irVertex.getPropertyValue(IgnoreSchedulingTempDataReceiverProperty.class).orElse(false)
        && dag.getIncomingEdgesOf(irVertex).stream()
        .filter(irEdge -> irEdge.getPropertyValue(CacheIDProperty.class).isPresent())
        .anyMatch(irEdge -> cacheIdToParallelism
            .containsKey(irEdge.getPropertyValue(CacheIDProperty.class).get()))) {
      builder.removeVertex(irVertex); // Ignore ghost vertex which was cached once.
      return;
    }

    dag.getIncomingEdgesOf(irVertex).stream()
        .forEach(edge -> {
      final Optional<UUID> cacheId = dag.getOutgoingEdgesOf(edge.getSrc()).stream()
          .filter(edgeToFilter -> edgeToFilter.getPropertyValue(CacheIDProperty.class).isPresent())
          .map(edgeToMap -> edgeToMap.getPropertyValue(CacheIDProperty.class).get())
          .findFirst();
      if (cacheId.isPresent() && cacheIdToParallelism.get(cacheId.get()) != null) { // Cached already.
        // Replace the vertex emitting cached edge with a cached source vertex.
        final IRVertex cachedDataRelayVertex = new CachedSourceVertex(cacheIdToParallelism.get(cacheId.get()));
        cachedDataRelayVertex.setPropertyPermanently(ParallelismProperty.of(cacheIdToParallelism.get(cacheId.get())));

        builder.addVertex(cachedDataRelayVertex);
        final IREdge newEdge = new IREdge(
            edge.getPropertyValue(CommunicationPatternProperty.class)
                .orElseThrow(() -> new RuntimeException("No communication pattern on an ir edge")),
            cachedDataRelayVertex,
            irVertex);
        edge.copyExecutionPropertiesTo(newEdge);
        newEdge.setProperty(CacheIDProperty.of(cacheId.get()));
        builder.connectVertices(newEdge);
        // Stop the recursion for this vertex.
      } else {
        final IRVertex srcVtx = edge.getSrc();
        builder.addVertex(srcVtx);
        builder.connectVertices(edge);
        addNonCachedVerticesAndEdges(dag, srcVtx, builder);
      }
    });
  }
}
