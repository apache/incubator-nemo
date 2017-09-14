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

import edu.snu.vortex.common.dag.Vertex;

import java.util.List;

/**
 * PhysicalStage.
 */
public final class PhysicalStage extends Vertex {
  private final List<TaskGroup> taskGroupList;
  private final int scheduleGroupIndex;

  public PhysicalStage(final String stageId,
                       final List<TaskGroup> taskGroupList,
                       final int scheduleGroupIndex) {
    super(stageId);
    this.taskGroupList = taskGroupList;
    this.scheduleGroupIndex = scheduleGroupIndex;
  }

  public List<TaskGroup> getTaskGroupList() {
    return taskGroupList;
  }

  public int getScheduleGroupIndex() {
    return scheduleGroupIndex;
  }

  @Override
  public String propertiesToJSON() {
    final StringBuilder sb = new StringBuilder();
    sb.append("{\"scheduleGroupIndex\": ").append(scheduleGroupIndex);
    sb.append(", \"taskGroupList\": ").append(taskGroupList);
    sb.append('}');
    return sb.toString();
  }
}
