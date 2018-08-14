/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package edu.snu.nemo.runtime.common.eventhandler;

import edu.snu.nemo.common.eventhandler.RuntimeEvent;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.runtime.common.plan.PhysicalPlan;

/**
 * An event for triggering dynamic optimization.
 */
public final class DynamicOptimizationEvent implements RuntimeEvent {
  private final PhysicalPlan physicalPlan;
  private final Object dynOptData;
  private final String taskId;
  private final String executorId;
  private final IREdge targetEdge;

  /**
   * Default constructor.
   * @param physicalPlan physical plan to be optimized.
   * @param taskId id of the task which triggered the dynamic optimization.
   * @param executorId the id of executor which executes {@code taskId}
   */
  public DynamicOptimizationEvent(final PhysicalPlan physicalPlan,
                                  final Object dynOptData,
                                  final String taskId,
                                  final String executorId,
                                  final IREdge targetEdge) {
    this.physicalPlan = physicalPlan;
    this.taskId = taskId;
    this.dynOptData = dynOptData;
    this.executorId = executorId;
    this.targetEdge = targetEdge;
  }

  /**
   * @return the physical plan to be optimized.
   */
  public PhysicalPlan getPhysicalPlan() {
    return this.physicalPlan;
  }

  /**
   * @return id of the task which triggered the dynamic optimization.
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

  public Object getDynOptData() {
    return this.dynOptData;
  }

  public IREdge getTargetEdge() {
    return this.targetEdge;
  }
}
