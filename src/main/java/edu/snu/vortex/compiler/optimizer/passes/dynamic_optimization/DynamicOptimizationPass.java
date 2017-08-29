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
package edu.snu.vortex.compiler.optimizer.passes.dynamic_optimization;

import edu.snu.vortex.runtime.common.plan.physical.PhysicalPlan;

import java.util.List;
import java.util.Map;

/**
 * Interface for dynamic optimization passes.
 */
public interface DynamicOptimizationPass {
  /**
   * A pass for dynamically optimizing a physical plan.
   * @param originalPlan original physical plan.
   * @param metricData metric data to dynamically optimize the physical plan upon.
   * @return the new physical plan after the dynamic optimization.
   * TODO #437: change this to IR DAG by using stage/scheduler domain info instead of the info in physical dag.
   */
  PhysicalPlan process(PhysicalPlan originalPlan, Map<String, List> metricData);
}
