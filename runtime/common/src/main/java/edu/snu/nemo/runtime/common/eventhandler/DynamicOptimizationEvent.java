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

import edu.snu.nemo.common.Pair;
import edu.snu.nemo.common.eventhandler.RuntimeEvent;
import edu.snu.nemo.runtime.common.plan.PhysicalPlan;

/**
 * An event for triggering dynamic optimization.
 */
public final class DynamicOptimizationEvent implements RuntimeEvent {
  private final PhysicalPlan physicalPlan;
  private final Object metricData;
  private final Pair<String, String> taskInfo;

  /**
   * Default constructor.
   * @param physicalPlan physical plan to be optimized.
   * @param taskInfo information of the task.
   */
  public DynamicOptimizationEvent(final PhysicalPlan physicalPlan,
                                  final Object metricData,
                                  final Pair<String, String> taskInfo) {
    this.physicalPlan = physicalPlan;
    this.metricData = metricData;
    this.taskInfo = taskInfo;
  }

  /**
   * @return the physical plan to be optimized.
   */
  public PhysicalPlan getPhysicalPlan() {
    return this.physicalPlan;
  }

  public Object getMetricData() {
    return this.metricData;
  }

  /**
   * @return the information of the task at which this optimization occurs: its name and its task ID.
   */
  public Pair<String, String> getTaskInfo() {
    return this.taskInfo;
  }
}
