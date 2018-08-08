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

import edu.snu.nemo.common.eventhandler.CompilerEvent;
import edu.snu.nemo.runtime.common.plan.PhysicalPlan;

/**
 * An event for updating the physical plan in the scheduler.
 */
public final class UpdatePhysicalPlanEvent implements CompilerEvent {
  private final PhysicalPlan newPhysicalPlan;
  private final String taskId;
  private final String executorId;

  /**
   * Constructor.
   * @param newPhysicalPlan the newly optimized physical plan.
   * @param taskId id of the task which triggered the dynamic optimization.
   * @param executorId the id of executor which executes {@code taskId}
   */
  UpdatePhysicalPlanEvent(final PhysicalPlan newPhysicalPlan,
                          final String taskId,
                          final String executorId) {
    this.newPhysicalPlan = newPhysicalPlan;
    this.taskId = taskId;
    this.executorId = executorId;
  }

  /**
   * @return the updated, newly optimized physical plan.
   */
  public PhysicalPlan getNewPhysicalPlan() {
    return this.newPhysicalPlan;
  }

  /**
   * @return id of the task which triggered the dynamic optimization
   */
  public String getTaskId() {
    return taskId;
  }

  /**
   * @return id of the executor which triggered the dynamic optimization
   */
  public String getExecutorId() {
    return executorId;
  }
}
