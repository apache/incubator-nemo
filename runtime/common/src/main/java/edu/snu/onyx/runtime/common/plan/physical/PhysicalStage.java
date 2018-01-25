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
package edu.snu.onyx.runtime.common.plan.physical;

import edu.snu.onyx.common.dag.Vertex;
import edu.snu.onyx.runtime.common.RuntimeIdGenerator;

import java.util.ArrayList;
import java.util.List;

/**
 * PhysicalStage.
 */
public final class PhysicalStage extends Vertex {
  private final TaskGroup taskGroup;
  private final int parallelism;
  private final int scheduleGroupIndex;

  /**
   * Constructor.
   *
   * @param stageId            id of the stage.
   * @param taskGroup          the task group of this stage.
   * @param parallelism        how many task groups will be executed in this stage.
   * @param scheduleGroupIndex the schedule group index.
   */
  public PhysicalStage(final String stageId,
                       final TaskGroup taskGroup,
                       final int parallelism,
                       final int scheduleGroupIndex) {
    super(stageId);
    this.taskGroup = taskGroup;
    this.parallelism = parallelism;
    this.scheduleGroupIndex = scheduleGroupIndex;
  }

  /**
   * @return the task group.
   */
  public TaskGroup getTaskGroup() {
    return taskGroup;
  }

  /**
   * @return the list of the task group IDs in this stage.
   */
  public List<String> getTaskGroupIds() {
    final List<String> taskGroupIds = new ArrayList<>();
    for (int taskGroupIdx = 0; taskGroupIdx < parallelism; taskGroupIdx++) {
      taskGroupIds.add(RuntimeIdGenerator.generateTaskGroupId(taskGroupIdx, getId()));
    }
    return taskGroupIds;
  }

  /**
   * @return the schedule group index.
   */
  public int getScheduleGroupIndex() {
    return scheduleGroupIndex;
  }

  @Override
  public String propertiesToJSON() {
    final StringBuilder sb = new StringBuilder();
    sb.append("{\"scheduleGroupIndex\": ").append(scheduleGroupIndex);
    sb.append(", \"taskGroup\": ").append(taskGroup);
    sb.append(", \"parallelism\": ").append(parallelism);
    sb.append('}');
    return sb.toString();
  }
}
