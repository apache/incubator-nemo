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

import com.google.common.collect.Lists;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.PartitionerProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.edge.executionproperty.DataFlowModelProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.ExecutorPlacementProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.ScheduleGroupIndexProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.StageIdProperty;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A pass for assigning each stages in schedule groups.
 * We traverse the DAG topologically to find the dependency information between stages and number them appropriately
 * to give correct order or schedule groups.
 */
public final class ScheduleGroupPass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public ScheduleGroupPass() {
    super(ScheduleGroupIndexProperty.class, Stream.of(
        StageIdProperty.class,
        DataCommunicationPatternProperty.class,
        ExecutorPlacementProperty.class,
        DataFlowModelProperty.class,
        PartitionerProperty.class,
        ParallelismProperty.class
    ).collect(Collectors.toSet()));
  }

  private static final int INITIAL_SCHEDULE_GROUP = 0;

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    // We assume that the input dag is tagged with stage ids.
    if (dag.getVertices().stream()
        .anyMatch(irVertex -> !irVertex.getPropertyValue(StageIdProperty.class).isPresent())) {
      throw new RuntimeException("There exists an IR vertex going through ScheduleGroupPass "
          + "without stage id tagged.");
    }

    // Map of stage id to the stage ids that it depends on.
    final Map<Integer, Set<Integer>> dependentStagesMap = new HashMap<>();
    dag.topologicalDo(irVertex -> {
      final Integer currentStageId = irVertex.getPropertyValue(StageIdProperty.class).get();
      dependentStagesMap.putIfAbsent(currentStageId, new HashSet<>());
      // while traversing, we find the stages that point to the current stage and add them to the list.
      dag.getIncomingEdgesOf(irVertex).stream()
          .map(IREdge::getSrc)
          .mapToInt(vertex -> vertex.getPropertyValue(StageIdProperty.class).get())
          .filter(n -> n != currentStageId)
          .forEach(n -> dependentStagesMap.get(currentStageId).add(n));
    });

    // Map to put our results in.
    final Map<Integer, Integer> stageIdToScheduleGroupIndexMap = new HashMap<>();

    // Calculate schedule group number of each stages step by step
    while (stageIdToScheduleGroupIndexMap.size() < dependentStagesMap.size()) {
      // This is to ensure that each iteration is making progress.
      // We ensure that the stageIdToScheduleGroupIdMap is increasing in size in each iteration.
      final Integer previousSize = stageIdToScheduleGroupIndexMap.size();
      dependentStagesMap.forEach((stageId, dependentStages) -> {
        if (!stageIdToScheduleGroupIndexMap.keySet().contains(stageId)
            && dependentStages.isEmpty()) { // initial source stages
          // initial source stages are indexed with schedule group 0.
          stageIdToScheduleGroupIndexMap.put(stageId, INITIAL_SCHEDULE_GROUP);
        } else if (!stageIdToScheduleGroupIndexMap.keySet().contains(stageId)
            && dependentStages.stream().allMatch(stageIdToScheduleGroupIndexMap::containsKey)) { // next stages
          // We find the maximum schedule group index from previous stages, and index current stage with that number +1.
          final Integer maxDependentSchedulerGroupIndex =
              dependentStages.stream()
                  .mapToInt(stageIdToScheduleGroupIndexMap::get)
                  .max().orElseThrow(() ->
                    new RuntimeException("A stage that is not a source stage much have dependent stages"));
          stageIdToScheduleGroupIndexMap.put(stageId, maxDependentSchedulerGroupIndex + 1);
        }
      });
      if (previousSize == stageIdToScheduleGroupIndexMap.size()) {
        throw new RuntimeException("Iteration for indexing schedule groups in "
            + ScheduleGroupPass.class.getSimpleName() + " is not making progress");
      }
    }

    // Reverse topologically traverse and match schedule group ids for those that have push edges in between
    Lists.reverse(dag.getTopologicalSort()).forEach(v -> {
      // get the destination vertices of the edges that are marked as push
      final List<IRVertex> pushConnectedVertices = dag.getOutgoingEdgesOf(v).stream()
          .filter(e -> DataFlowModelProperty.Value.Push.equals(e.getPropertyValue(DataFlowModelProperty.class).get()))
          .map(IREdge::getDst)
          .collect(Collectors.toList());
      if (!pushConnectedVertices.isEmpty()) { // if we need to do something,
        // we find the min value of the destination schedule groups.
        final Integer newSchedulerGroupIndex = pushConnectedVertices.stream()
            .mapToInt(irVertex -> stageIdToScheduleGroupIndexMap
                .get(irVertex.getPropertyValue(StageIdProperty.class).get()))
            .min().orElseThrow(() -> new RuntimeException("a list was not empty, but produced an empty result"));
        // overwrite
        final Integer originalScheduleGroupIndex = stageIdToScheduleGroupIndexMap
            .get(v.getPropertyValue(StageIdProperty.class).get());
        stageIdToScheduleGroupIndexMap.replace(v.getPropertyValue(StageIdProperty.class).get(), newSchedulerGroupIndex);
        // shift those if it came too far
        if (stageIdToScheduleGroupIndexMap.values().stream()
            .noneMatch(stageIndex -> stageIndex.equals(originalScheduleGroupIndex))) { // if it doesn't exist
          stageIdToScheduleGroupIndexMap.replaceAll((stageId, scheduleGroupIndex) -> {
            if (scheduleGroupIndex > originalScheduleGroupIndex) {
              return scheduleGroupIndex - 1; // we shift schedule group indexes by one.
            } else {
              return scheduleGroupIndex;
            }
          });
        }
      }
    });

    // do the tagging
    dag.topologicalDo(irVertex -> irVertex.setProperty(ScheduleGroupIndexProperty.of(
        stageIdToScheduleGroupIndexMap.get(irVertex.getPropertyValue(StageIdProperty.class).get()))));

    return dag;
  }
}
