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
package edu.snu.nemo.runtime.common.eventhandler;

import edu.snu.nemo.common.Pair;
import edu.snu.nemo.common.eventhandler.CompilerEvent;
import edu.snu.nemo.runtime.common.plan.PhysicalPlan;

/**
 * An event for updating the physical plan in the scheduler.
 */
public final class UpdatePhysicalPlanEvent implements CompilerEvent {
  private final PhysicalPlan newPhysicalPlan;
  private final Pair<String, String> taskInfo;

  /**
   * Constructor.
   * @param newPhysicalPlan the newly optimized physical plan.
   * @param taskInfo information of the task at which this optimization occurs: its name and its task ID.
   */
  UpdatePhysicalPlanEvent(final PhysicalPlan newPhysicalPlan,
                          final Pair<String, String> taskInfo) {
    this.newPhysicalPlan = newPhysicalPlan;
    this.taskInfo = taskInfo;
  }

  /**
   * @return the updated, newly optimized physical plan.
   */
  public PhysicalPlan getNewPhysicalPlan() {
    return this.newPhysicalPlan;
  }

  /**
   * @return the information of the task at which this optimization occurs: its name and its task ID.
   */
  public Pair<String, String> getTaskInfo() {
    return this.taskInfo;
  }
}
