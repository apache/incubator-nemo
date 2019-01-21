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
package org.apache.nemo.runtime.common.eventhandler;

import org.apache.nemo.common.eventhandler.RuntimeEvent;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.common.plan.StageEdge;

import java.util.Set;

/**
 * An event for triggering dynamic optimization.
 */
public final class DynamicOptimizationEvent implements RuntimeEvent {
  private final PhysicalPlan physicalPlan;
  private final Object dynOptData;
  private final String taskId;
  private final String executorId;
  private final Set<StageEdge> targetEdges;

  /**
   * Default constructor.
   *
   * @param physicalPlan the physical plan to be optimized.
   * @param dynOptData   the metric data.
   * @param taskId       the ID of the task which triggered the dynamic optimization.
   * @param executorId   the ID of executor which executes {@code taskId}
   * @param targetEdges  the target edges.
   */
  public DynamicOptimizationEvent(final PhysicalPlan physicalPlan,
                                  final Object dynOptData,
                                  final String taskId,
                                  final String executorId,
                                  final Set<StageEdge> targetEdges) {
    this.physicalPlan = physicalPlan;
    this.taskId = taskId;
    this.dynOptData = dynOptData;
    this.executorId = executorId;
    this.targetEdges = targetEdges;
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

  public Set<StageEdge> getTargetEdges() {
    return this.targetEdges;
  }
}
