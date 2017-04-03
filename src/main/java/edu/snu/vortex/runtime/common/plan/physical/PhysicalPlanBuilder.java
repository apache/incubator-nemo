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

  /**
   * A list of stages. Each stage is represented by a list task groups to be executed for the stage.
   */
  private final List<List<TaskGroup>> taskGroupsByStage;
  private List<TaskGroup> currentStageTaskGroups;

  /**
   * Builds a {@link PhysicalPlan}.
   * @param executionPlanId ID of the execution plan this physical plan corresponds to.
   */
  public PhysicalPlanBuilder(final String executionPlanId) {
    this.physicalPlanId = executionPlanId;
    this.taskGroupsByStage = new LinkedList<>();
  }

  public void createNewStage(final int parallelism) {
    currentStageTaskGroups = new ArrayList<>(parallelism);
    taskGroupsByStage.add(currentStageTaskGroups);
  }

  public void addTaskGroupToCurrentStage(final TaskGroup taskGroup) {
    currentStageTaskGroups.add(taskGroup);
  }

  public PhysicalPlan build() {
    return new PhysicalPlan(physicalPlanId, taskGroupsByStage);
  }
}
