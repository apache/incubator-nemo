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
package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.Util;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.dag.Edge;
import org.apache.nemo.common.dag.Vertex;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DataFlowProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ScheduleGroupProperty;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A pass for assigning each stages in schedule groups.
 *
 * TODO #347: IRDAG#partitionAcyclically
 * This code can be greatly simplified...
 *
 * <h3>Rules</h3>
 * <ul>
 *   <li>Vertices connected with push edges must be assigned same ScheduleGroup.</li>
 *   <li>For pull edges,
 *     <ul>
 *       <li>if the destination of the edge depends on multiple ScheduleGroups, split ScheduleGroup by the edge.</li>
 *       <li>if the edge is broadcast type and {@code allowBroadcastWithinScheduleGroup} is {@code false},
 *       split ScheduleGroup by the edge.</li>
 *       <li>if the edge is shuffle type and {@code allowShuffleWithinScheduleGroup} is {@code false},
 *       split ScheduleGroup by the edge.</li>
 *       <li>if the destination of the edge has multiple inEdges, split ScheduleGroup by the edge.</li>
 *       <li>Otherwise, the source and the destination of the edge should be assigned same ScheduleGroup.</li>
 *     </ul>
 *   </li>
 * </ul>
 */
@Annotates(ScheduleGroupProperty.class)
@Requires({CommunicationPatternProperty.class, DataFlowProperty.class})
public final class DefaultScheduleGroupPass extends AnnotatingPass {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultScheduleGroupPass.class.getName());

  private final boolean allowBroadcastWithinScheduleGroup;
  private final boolean allowShuffleWithinScheduleGroup;
  private final boolean allowMultipleInEdgesWithinScheduleGroup;

  /**
   * Default constructor.
   */
  public DefaultScheduleGroupPass() {
    this(true, true, true);
  }

  /**
   * Constructor.
   * @param allowBroadcastWithinScheduleGroup whether to allow Broadcast edges within a ScheduleGroup or not
   * @param allowShuffleWithinScheduleGroup whether to allow Shuffle edges within a ScheduleGroup or not
   * @param allowMultipleInEdgesWithinScheduleGroup whether to allow vertices with multiple dependencies or not
   */
  public DefaultScheduleGroupPass(final boolean allowBroadcastWithinScheduleGroup,
                                  final boolean allowShuffleWithinScheduleGroup,
                                  final boolean allowMultipleInEdgesWithinScheduleGroup) {
    super(DefaultScheduleGroupPass.class);
    this.allowBroadcastWithinScheduleGroup = allowBroadcastWithinScheduleGroup;
    this.allowShuffleWithinScheduleGroup = allowShuffleWithinScheduleGroup;
    this.allowMultipleInEdgesWithinScheduleGroup = allowMultipleInEdgesWithinScheduleGroup;
  }

  @Override
  public IRDAG apply(final IRDAG dag) {
    final Map<IRVertex, Integer> irVertexToGroupIdMap = new HashMap<>();
    final Map<Integer, List<IRVertex>> groupIdToVertices = new HashMap<>();

    // Step 1: Compute schedule groups
    final MutableInt lastGroupId = new MutableInt(0);
    dag.topologicalDo(irVertex -> {
      final int curId;
      if (!irVertexToGroupIdMap.containsKey(irVertex)) {
        lastGroupId.increment();
        irVertexToGroupIdMap.put(irVertex, lastGroupId.intValue());
        curId = lastGroupId.intValue();
      } else {
        curId = irVertexToGroupIdMap.get(irVertex);
      }
      groupIdToVertices.putIfAbsent(curId, new ArrayList<>());
      groupIdToVertices.get(curId).add(irVertex);

      final List<IREdge> allOutEdges = dag.getOutgoingEdgesOf(irVertex);
      final List<IREdge> noCycleOutEdges = allOutEdges.stream().filter(curEdge -> {
        final List<IREdge> outgoingEdgesWithoutCurEdge = new ArrayList<>(allOutEdges);
        outgoingEdgesWithoutCurEdge.remove(curEdge);
        return outgoingEdgesWithoutCurEdge.stream()
          .map(IREdge::getDst)
          .flatMap(dst -> dag.getDescendants(dst.getId()).stream())
          .noneMatch(descendant -> descendant.equals(curEdge.getDst()));
      }).collect(Collectors.toList());

      final List<IRVertex> pushNoCycleOutEdgeDsts = noCycleOutEdges.stream()
        .filter(e -> DataFlowProperty.Value.Push.equals(e.getPropertyValue(DataFlowProperty.class).get()))
        .map(IREdge::getDst)
        .collect(Collectors.toList());

      pushNoCycleOutEdgeDsts.forEach(dst -> irVertexToGroupIdMap.put(dst, curId));
    });

    // Step 2: Topologically sort schedule groups
    final Map<Integer, List<Pair>> vIdTogId = irVertexToGroupIdMap.entrySet().stream()
      .map(entry -> Pair.of(entry.getKey().getId(), entry.getValue()))
      .collect(Collectors.groupingBy(p -> (Integer) ((Pair) p).right()));

    final DAGBuilder<ScheduleGroup, ScheduleGroupEdge> builder = new DAGBuilder<>();
    final Map<Integer, ScheduleGroup> idToGroup = new HashMap<>();

    // ScheduleGroups
    groupIdToVertices.forEach((groupId, vertices) -> {
      final ScheduleGroup sg = new ScheduleGroup(groupId);
      idToGroup.put(groupId, sg);
      sg.vertices.addAll(vertices);
      builder.addVertex(sg);
    });

    // ScheduleGroupEdges
    irVertexToGroupIdMap.forEach((vertex, groupId) -> {
      dag.getIncomingEdgesOf(vertex).stream()
        .filter(inEdge -> !groupIdToVertices.get(groupId).contains(inEdge.getSrc()))
        .map(inEdge -> new ScheduleGroupEdge(
          idToGroup.get(irVertexToGroupIdMap.get(inEdge.getSrc())),
          idToGroup.get(irVertexToGroupIdMap.get(inEdge.getDst()))))
        .forEach(builder::connectVertices);
    });

    // Step 3: Actually set new schedule group properties based on topological ordering
    final MutableInt actualScheduleGroup = new MutableInt(0);
    final DAG<ScheduleGroup, ScheduleGroupEdge> sgDAG = builder.buildWithoutSourceSinkCheck();
    final List<ScheduleGroup> sorted = sgDAG.getTopologicalSort();
    sorted.stream()
      .map(sg -> groupIdToVertices.get(sg.getScheduleGroupId()))
      .forEach(vertices -> {
        vertices.forEach(vertex -> {
          vertex.setProperty(ScheduleGroupProperty.of(actualScheduleGroup.intValue()));
        });
        actualScheduleGroup.increment();
      });

    return dag;
  }

  /**
   * Vertex in ScheduleGroup DAG.
   */
  private static final class ScheduleGroup extends Vertex {
    private final Set<IRVertex> vertices = new HashSet<>();
    private final Set<ScheduleGroup> scheduleGroupsTo = new HashSet<>();
    private final Set<ScheduleGroup> scheduleGroupsFrom = new HashSet<>();
    private final int scheduleGroupId;

    /**
     * Constructor.
     */
    ScheduleGroup(final int groupId) {
      super(String.format("ScheduleGroup%d", groupId));
      this.scheduleGroupId = groupId;
    }

    public int getScheduleGroupId() {
      return scheduleGroupId;
    }

    @Override
    public ObjectNode getPropertiesAsJsonNode() {
      final ObjectMapper mapper = new ObjectMapper();
      final ObjectNode node = mapper.createObjectNode();
      node.put("transform", Util.stringifyIRVertexIds(vertices));
      return node;
    }
  }

  /**
   * Edge in ScheduleGroup DAG.
   */
  private static final class ScheduleGroupEdge extends Edge<ScheduleGroup> {
    private static int nextScheduleGroupEdgeId = 0;

    /**
     * Constructor.
     *
     * @param src source vertex.
     * @param dst destination vertex.
     */
    ScheduleGroupEdge(final ScheduleGroup src, final ScheduleGroup dst) {
      super(String.format("ScheduleGroupEdge-%d", nextScheduleGroupEdgeId++), src, dst);
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final ScheduleGroupEdge that = (ScheduleGroupEdge) o;
      return this.getSrc().equals(that.getSrc()) && this.getDst().equals(that.getDst());
    }

    @Override
    public int hashCode() {
      return getSrc().hashCode() + 31 * getDst().hashCode();
    }
  }
}
