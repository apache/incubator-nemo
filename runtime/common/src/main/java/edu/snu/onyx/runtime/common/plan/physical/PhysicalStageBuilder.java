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

import java.util.ArrayList;
import java.util.List;

/**
 * PhysicalStageBuilder.
 */
public final class PhysicalStageBuilder {
  private final String id;
  private final List<TaskGroup> taskGroupList;
  private final int scheduleGroupIndex;

  /**
   * Constructor.
   * @param stageId id of the stage.
   * @param parallelism parallelism of the stage.
   * @param scheduleGroupIndex the schedule group index.
   */
  public PhysicalStageBuilder(final String stageId,
                              final int parallelism,
                              final int scheduleGroupIndex) {
    this.id = stageId;
    this.taskGroupList = new ArrayList<>(parallelism);
    this.scheduleGroupIndex = scheduleGroupIndex;
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("PhysicalStage{");
    sb.append("id='").append(id).append('\'');
    sb.append("scheduleGroupIndex='").append(scheduleGroupIndex).append('\'');
    sb.append(", taskGroupList=").append(taskGroupList);
    sb.append('}');
    return sb.toString();
  }

  /**
   * Add a taskGroup to the stage.
   * @param taskGroup the taskGroup to add to the stage.
   */
  public void addTaskGroup(final TaskGroup taskGroup) {
    taskGroupList.add(taskGroup);
  }

  /**
   * @return the built PhysicalStage.
   */
  public PhysicalStage build() {
    return new PhysicalStage(id, taskGroupList, scheduleGroupIndex);
  }
}
