/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating;

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.common.dag.Edge;
import edu.snu.nemo.common.dag.Vertex;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.edge.executionproperty.DataFlowModelProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.ScheduleGroupIndexProperty;
import org.apache.commons.lang3.mutable.MutableInt;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A pass for assigning each stages in schedule groups.
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
public final class DefaultScheduleGroupPass extends AnnotatingPass {

  private final boolean allowBroadcastWithinScheduleGroup;
  private final boolean allowShuffleWithinScheduleGroup;
  private final boolean allowMultipleInEdgesWithinScheduleGroup;

  /**
   * Default constructor.
   */
  public DefaultScheduleGroupPass() {
    this(false, false, true);
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
    super(ScheduleGroupIndexProperty.class, Stream.of(
        DataCommunicationPatternProperty.class,
        DataFlowModelProperty.class
    ).collect(Collectors.toSet()));
    this.allowBroadcastWithinScheduleGroup = allowBroadcastWithinScheduleGroup;
    this.allowShuffleWithinScheduleGroup = allowShuffleWithinScheduleGroup;
    this.allowMultipleInEdgesWithinScheduleGroup = allowMultipleInEdgesWithinScheduleGroup;
  }


  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    final Map<IRVertex, ScheduleGroup> irVertexToScheduleGroupMap = new HashMap<>();
    final Set<ScheduleGroup> scheduleGroups = new HashSet<>();
    dag.topologicalDo(irVertex -> {
      // Base case: for root vertices
      if (!irVertexToScheduleGroupMap.containsKey(irVertex)) {
        final ScheduleGroup newScheduleGroup = new ScheduleGroup();
        scheduleGroups.add(newScheduleGroup);
        newScheduleGroup.vertices.add(irVertex);
        irVertexToScheduleGroupMap.put(irVertex, newScheduleGroup);
      }
      // Get scheduleGroupIndex
      final ScheduleGroup scheduleGroup = irVertexToScheduleGroupMap.get(irVertex);
      if (scheduleGroup == null) {
        throw new RuntimeException(String.format("ScheduleGroup must be set for %s", irVertex));
      }
      // Step case: inductively assign ScheduleGroup
      for (final IREdge edge : dag.getOutgoingEdgesOf(irVertex)) {
        final IRVertex connectedIRVertex = edge.getDst();
        // Skip if some vertices that connectedIRVertex depends on do not have assigned a ScheduleGroup
        boolean skip = false;
        for (final IREdge edgeToConnectedIRVertex : dag.getIncomingEdgesOf(connectedIRVertex)) {
          if (!irVertexToScheduleGroupMap.containsKey(edgeToConnectedIRVertex.getSrc())) {
            // connectedIRVertex will be covered when edgeToConnectedIRVertex.getSrc() is visited
            skip = true;
            break;
          }
        }
        if (skip) {
          continue;
        }
        // Now we can assure that all vertices that connectedIRVertex depends on have assigned a ScheduleGroup

        // Get ScheduleGroup(s) that push data to the connectedIRVertex
        final Set<ScheduleGroup> pushScheduleGroups = new HashSet<>();
        for (final IREdge edgeToConnectedIRVertex : dag.getIncomingEdgesOf(connectedIRVertex)) {
          if (edgeToConnectedIRVertex.getPropertyValue(DataFlowModelProperty.class)
              .orElseThrow(() -> new RuntimeException(String.format("DataFlowModelProperty for %s must be set",
                  edgeToConnectedIRVertex.getId()))) == DataFlowModelProperty.Value.Push) {
            pushScheduleGroups.add(irVertexToScheduleGroupMap.get(edgeToConnectedIRVertex.getSrc()));
          }
        }
        if (pushScheduleGroups.isEmpty()) {
          // If allowMultipleInEdgesWithinScheduleGroup is false and connectedIRVertex depends on multiple vertices,
          // it should be a member of new ScheduleGroup
          boolean mergability = allowMultipleInEdgesWithinScheduleGroup
              || dag.getIncomingEdgesOf(connectedIRVertex).size() <= 1;
          for (final IREdge edgeToConnectedIRVertex : dag.getIncomingEdgesOf(connectedIRVertex)) {
            if (!mergability) {
              break;
            }
            final ScheduleGroup anotherDependency = irVertexToScheduleGroupMap.get(edgeToConnectedIRVertex.getSrc());
            if (!scheduleGroup.equals(anotherDependency)) {
              // Since connectedIRVertex depends on multiple ScheduleGroups, connectedIRVertex must be a member of
              // new ScheduleGroup
              mergability = false;
            }
            final DataCommunicationPatternProperty.Value communicationPattern = edgeToConnectedIRVertex
                .getPropertyValue(DataCommunicationPatternProperty.class).orElseThrow(
                    () -> new RuntimeException(String.format("DataCommunicationPatternProperty for %s must be set",
                        edgeToConnectedIRVertex.getId())));
            if (!allowBroadcastWithinScheduleGroup
                && communicationPattern == DataCommunicationPatternProperty.Value.BroadCast) {
              mergability = false;
            }
            if (!allowShuffleWithinScheduleGroup
                && communicationPattern == DataCommunicationPatternProperty.Value.Shuffle) {
              mergability = false;
            }
          }

          if (mergability) {
            // Merge into the existing scheduleGroup
            scheduleGroup.vertices.add(connectedIRVertex);
            irVertexToScheduleGroupMap.put(connectedIRVertex, scheduleGroup);
          } else {
            // Create a new ScheduleGroup
            final ScheduleGroup newScheduleGroup = new ScheduleGroup();
            scheduleGroups.add(newScheduleGroup);
            newScheduleGroup.vertices.add(connectedIRVertex);
            irVertexToScheduleGroupMap.put(connectedIRVertex, newScheduleGroup);
            for (final IREdge edgeToConnectedIRVertex : dag.getIncomingEdgesOf(connectedIRVertex)) {
              final ScheduleGroup src = irVertexToScheduleGroupMap.get(edgeToConnectedIRVertex.getSrc());
              final ScheduleGroup dst = newScheduleGroup;
              src.scheduleGroupsTo.add(dst);
              dst.scheduleGroupsFrom.add(src);
            }
          }
        } else {
          // If there are multiple ScheduleGroups that push data to connectedIRVertex, merge them
          final Iterator<ScheduleGroup> pushScheduleGroupIterator = pushScheduleGroups.iterator();
          final ScheduleGroup pushScheduleGroup = pushScheduleGroupIterator.next();
          while (pushScheduleGroupIterator.hasNext()) {
            final ScheduleGroup anotherPushScheduleGroup = pushScheduleGroupIterator.next();
            anotherPushScheduleGroup.vertices.forEach(pushScheduleGroup.vertices::add);
            scheduleGroups.remove(anotherPushScheduleGroup);
            for (final ScheduleGroup src : anotherPushScheduleGroup.scheduleGroupsFrom) {
              final ScheduleGroup dst = anotherPushScheduleGroup;
              final ScheduleGroup newDst = pushScheduleGroup;
              src.scheduleGroupsTo.remove(dst);
              src.scheduleGroupsTo.add(newDst);
              newDst.scheduleGroupsFrom.add(src);
            }
            for (final ScheduleGroup dst : anotherPushScheduleGroup.scheduleGroupsTo) {
              final ScheduleGroup src = anotherPushScheduleGroup;
              final ScheduleGroup newSrc = pushScheduleGroup;
              dst.scheduleGroupsFrom.remove(src);
              dst.scheduleGroupsFrom.add(newSrc);
              newSrc.scheduleGroupsTo.add(dst);
            }
          }
          // Add connectedIRVertex into the merged pushScheduleGroup
          pushScheduleGroup.vertices.add(connectedIRVertex);
          irVertexToScheduleGroupMap.put(connectedIRVertex, pushScheduleGroup);
        }
      }
    });

    // Assign ScheduleGroupIndex property based on topology of ScheduleGroups
    final MutableInt currentScheduleGroupIndex = new MutableInt(getNextScheudleGroupIndex(dag.getVertices()));
    final DAGBuilder<ScheduleGroup, ScheduleGroupEdge> scheduleGroupDAGBuilder = new DAGBuilder<>();
    scheduleGroups.forEach(scheduleGroupDAGBuilder::addVertex);
    scheduleGroups.forEach(src -> src.scheduleGroupsTo
        .forEach(dst -> scheduleGroupDAGBuilder.connectVertices(new ScheduleGroupEdge(src, dst))));
    scheduleGroupDAGBuilder.build().topologicalDo(scheduleGroup -> {
      boolean usedCurrentIndex = false;
      for (final IRVertex irVertex : scheduleGroup.vertices) {
        if (!irVertex.getPropertyValue(ScheduleGroupIndexProperty.class).isPresent()) {
          irVertex.getExecutionProperties().put(ScheduleGroupIndexProperty.of(currentScheduleGroupIndex.getValue()));
          usedCurrentIndex = true;
        }
      }
      if (usedCurrentIndex) {
        currentScheduleGroupIndex.increment();
      }
    });
    return dag;
  }

  /**
   * Determines the range of {@link ScheduleGroupIndexProperty} value that will prevent collision
   * with the existing {@link ScheduleGroupIndexProperty}.
   * @param irVertexCollection collection of {@link IRVertex}
   * @return the minimum value for the {@link ScheduleGroupIndexProperty} that won't collide with the existing values
   */
  private int getNextScheudleGroupIndex(final Collection<IRVertex> irVertexCollection) {
    int nextScheduleGroupIndex = 0;
    for (final IRVertex irVertex : irVertexCollection) {
      final Optional<Integer> scheduleGroupIndex = irVertex.getPropertyValue(ScheduleGroupIndexProperty.class);
      if (scheduleGroupIndex.isPresent()) {
        nextScheduleGroupIndex = Math.max(scheduleGroupIndex.get() + 1, nextScheduleGroupIndex);
      }
    }
    return nextScheduleGroupIndex;
  }

  /**
   * Vertex in ScheduleGroup DAG.
   */
  private static final class ScheduleGroup extends Vertex {
    private static int nextScheduleGroupId = 0;
    private final Set<IRVertex> vertices = new HashSet<>();
    private final Set<ScheduleGroup> scheduleGroupsTo = new HashSet<>();
    private final Set<ScheduleGroup> scheduleGroupsFrom = new HashSet<>();

    /**
     * Constructor.
     */
    ScheduleGroup() {
      super(String.format("ScheduleGroup-%d", nextScheduleGroupId++));
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
