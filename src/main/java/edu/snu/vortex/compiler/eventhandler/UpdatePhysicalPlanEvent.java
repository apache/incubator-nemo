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
package edu.snu.vortex.compiler.eventhandler;

import edu.snu.vortex.common.Pair;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalPlan;
import edu.snu.vortex.runtime.common.plan.physical.TaskGroup;

/**
 * An event for updating the physical plan in the scheduler.
 */
public final class UpdatePhysicalPlanEvent implements CompilerEvent {
  private final PhysicalPlan newPhysicalPlan;
  private final Pair<String, TaskGroup> taskInfo;

  UpdatePhysicalPlanEvent(final PhysicalPlan newPhysicalPlan,
                                 final Pair<String, TaskGroup> taskInfo) {
    this.newPhysicalPlan = newPhysicalPlan;
    this.taskInfo = taskInfo;
  }

  public PhysicalPlan getNewPhysicalPlan() {
    return this.newPhysicalPlan;
  }

  public Pair<String, TaskGroup> getTaskInfo() {
    return this.taskInfo;
  }
}
