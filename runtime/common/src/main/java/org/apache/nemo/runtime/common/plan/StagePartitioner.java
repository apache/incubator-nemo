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
package org.apache.nemo.runtime.common.plan;

import net.jcip.annotations.ThreadSafe;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.ir.vertex.utility.ConditionalRouterVertex;
import org.apache.nemo.common.ir.vertex.utility.SrcStreamVertex;
import org.apache.nemo.common.ir.vertex.utility.StateMergerVertex;
import org.apache.nemo.common.ir.vertex.utility.StreamVertex;
import org.apache.nemo.conf.EvalConf;
import org.apache.reef.annotations.audience.DriverSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A function that is responsible for stage partitioning on IR DAG.
 * Each stage becomes maximal set of {@link IRVertex} such that
 * <ul>
 *   <li>branches and non-OneToOne edges are not allowed within a stage, and</li>
 *   <li>all vertices in a stage should have same {@link VertexExecutionProperty} map,
 *   except for the ignored properties.</li>
 * </ul>
 */
@DriverSide
@ThreadSafe
public final class StagePartitioner implements Function<IRDAG, Map<IRVertex, Integer>> {
  private static final Logger LOG = LoggerFactory.getLogger(StagePartitioner.class.getName());
  private final Set<Class<? extends VertexExecutionProperty>> ignoredPropertyKeys = ConcurrentHashMap.newKeySet();
  private final MutableInt nextStageIndex = new MutableInt(0);

  private final EvalConf evalConf;

  public StagePartitioner(final EvalConf evalConf) {
    this.evalConf = evalConf;
  }

  /**
   * By default, the stage partitioner merges two vertices into one stage if and only if the two vertices have
   * same set of {@link VertexExecutionProperty}.
   * Invoking this method will make the stage partitioner ignore a specific property during comparing
   * the execution property maps.
   * @param ignoredPropertyKey a property that will be ignored during the stage partitioning.
   */
  public void addIgnoredPropertyKey(final Class<? extends VertexExecutionProperty> ignoredPropertyKey) {
    ignoredPropertyKeys.add(ignoredPropertyKey);
  }

  /**
   * @param irDAG IR DAG to perform stage partition on.
   * @return a map between IR vertex and the corresponding stage id
   */
  @Override
  public Map<IRVertex, Integer> apply(final IRDAG irDAG) {
    final Map<IRVertex, Integer> vertexToStageIdMap = new HashMap<>();
    irDAG.topologicalDo(irVertex -> {
      // Base case: for root vertices
      boolean isRoot = false;
      if (vertexToStageIdMap.get(irVertex) == null) {
        vertexToStageIdMap.put(irVertex, nextStageIndex.getValue());
        isRoot = true;
      }

      // Get stage id of irVertex
      final int stageId = vertexToStageIdMap.get(irVertex);
      // Step case: inductively assign stage ids based on mergeability with irVertex
      for (final IREdge edge : irDAG.getOutgoingEdgesOf(irVertex)) {
        final IRVertex connectedIRVertex = edge.getDst();
        // Skip if it already has been assigned stageId
        if (vertexToStageIdMap.containsKey(connectedIRVertex)) {
          continue;
        }
        // Assign stageId
        /*
        if (isRoot) {
          vertexToStageIdMap.put(connectedIRVertex, nextStageIndex.getValue());
          nextStageIndex.increment();
        } else
        */
        if (testMergeability(edge, irDAG)) {
          vertexToStageIdMap.put(connectedIRVertex, stageId);
        } else {
          vertexToStageIdMap.put(connectedIRVertex, nextStageIndex.incrementAndGet());
        }
      }
    });
    return vertexToStageIdMap;
  }

  /**
   * @param edge an {@link IREdge}.
   * @param dag IR DAG which contains {@code edge}
   * @return {@code true} if and only if the source and the destination vertex of the edge can be merged into one stage.
   */
  private boolean testMergeability(final IREdge edge, final IRDAG dag) {

    // TODO:remove
      // QUERY0 test!!!
    /*
    if (edge.getDst().getId().equals("vertex6")) {
      return false;
    }
    */

    // for query 4
    if (evalConf.queryId == 4 && edge.getDst().getId().equals("vertex14")
      || evalConf.queryId == 4 && edge.getDst().getId().equals("vertex15")
      || evalConf.queryId == 4 && edge.getDst().getId().equals("vertex25")) {
      return false;
    }

    if (evalConf.queryId == 6 && edge.getDst().getId().equals("vertex14")
      || evalConf.queryId == 6 && edge.getDst().getId().equals("vertex15")) {
      //|| evalConf.queryId == 6 && edge.getDst().getId().equals("vertex23")) {
      return false;
    }

    if (evalConf.queryId == 13 && edge.getDst().getId().equals("vertex14")
      || evalConf.queryId == 13 && edge.getDst().getId().equals("vertex15")) {
      return false;
    }

    // If the destination vertex has multiple inEdges, return false
    if (dag.getIncomingEdgesOf(edge.getDst()).size() > 1) {
      LOG.info("StagePartitioner Src {}, Dst {} getIncomingEdge > 1", edge.getSrc(), edge.getDst());
      return false;
    }
    // If the edge is not OneToOne, return false
    if (edge.getPropertyValue(CommunicationPatternProperty.class).get()
        != CommunicationPatternProperty.Value.OneToOne) {
      LOG.info("StagePartitioner Src {}, Dst {} edge != o2o", edge.getSrc(), edge.getDst());
      return false;
    }

    if (edge.getSrc() instanceof StreamVertex) {
      // create a new task for stream vertex
      LOG.info("StagePartitioner Src {}, Dst {}  edge.getSrc() StreamVertex", edge.getSrc(), edge.getDst());
      return false;
    }

    if (edge.getDst() instanceof StreamVertex && !(edge.getSrc() instanceof SourceVertex)) {
      LOG.info("StagePartitioner Src {}, Dst {}  edge.getDst() StreamVertex", edge.getSrc(), edge.getDst());
      return false;
    }

    if (edge.getDst() instanceof SrcStreamVertex) {
      LOG.info("StagePartitioner Src {}, Dst {}  edge.getDst() SrcStreamVertex", edge.getSrc(), edge.getDst());
      return true;
    }


    if (edge.getSrc() instanceof ConditionalRouterVertex) {
      // create a new task for stream vertex
      LOG.info("StagePartitioner Src {}, Dst {}  edge.getSrc() CRVertex", edge.getSrc(), edge.getDst());
      return false;
    }

    if (edge.getDst() instanceof ConditionalRouterVertex)  {
      LOG.info("StagePartitioner Src {}, Dst {}  edge.getDst() CRVertex", edge.getSrc(), edge.getDst());
      return false;
    }


    if (edge.getSrc() instanceof StateMergerVertex) {
      // create a new task for stream vertex
      LOG.info("StagePartitioner Src {}, Dst {}  edge.getSrc() SMVertex", edge.getSrc(), edge.getDst());
      return false;
    }

    if (edge.getDst() instanceof StateMergerVertex)  {
      LOG.info("StagePartitioner Src {}, Dst {}  edge.getDst() SMVertex", edge.getSrc(), edge.getDst());
      return false;
    }

    // Return true if and only if the execution properties of the two vertices are compatible
    final boolean val =  getStageProperties(edge.getSrc()).equals(getStageProperties(edge.getDst()));
    if (!val) {
      LOG.info("StagePartitioner Src {}, Dst {} getStageProperties {}, src {}, dst {}",
        edge.getSrc(), edge.getDst(), val, getStageProperties(edge.getSrc()), getStageProperties(edge.getDst()));
    } else {
      LOG.info("StagePartitioner Src {}, Dst {} getStageProperties {}", edge.getSrc(), edge.getDst(), val);
    }
    return val;
  }

  /**
   * @param vertex a vertex in a stage
   * @return set of stage-level properties for the stage
   */
  public Set<VertexExecutionProperty> getStageProperties(final IRVertex vertex) {
    return vertex.getExecutionProperties().stream()
        .filter(p -> !ignoredPropertyKeys.contains(p.getClass()))
        .collect(Collectors.toSet());
  }
}
