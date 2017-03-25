/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.vortex.runtime.common.plan.physical;

import java.util.*;

/**
 * PhysicalPlanBuilder.
 */
public final class PhysicalPlanBuilder {
  private String physicalPlanId;
  private final Map<String, List<TaskGroup>> stageIdToTaskGroups;

  /**
   * Builds a {@link PhysicalPlan}.
   * @param executionPlanId ID of the execution plan this physical plan corresponds to.
   */
  public PhysicalPlanBuilder(final String executionPlanId) {
    this.physicalPlanId = executionPlanId;
    this.stageIdToTaskGroups = new HashMap<>();
  }

  public void createNewStage(final String runtimeStageId,
                             final int parallelism) {
    stageIdToTaskGroups.put(runtimeStageId, new ArrayList<>(parallelism));
  }

  public void addTaskGroupToStage(final String runtimeStageId,
                                  final TaskGroup taskGroup) {
    stageIdToTaskGroups.get(runtimeStageId).add(taskGroup);
  }

  public PhysicalPlan build() {
    return new PhysicalPlan(physicalPlanId, stageIdToTaskGroups);
  }
}
