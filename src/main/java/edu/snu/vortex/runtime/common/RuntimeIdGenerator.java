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
package edu.snu.vortex.runtime.common;


import java.util.concurrent.atomic.AtomicInteger;

/**
 * ID Generator.
 */
public final class RuntimeIdGenerator {
  private static AtomicInteger executionPlanIdGenerator = new AtomicInteger(1);
  private static AtomicInteger runtimeStageIdGenerator = new AtomicInteger(1);

  private RuntimeIdGenerator() {
  }

  /**
   * Generates the ID for {@link edu.snu.vortex.runtime.common.execplan.ExecutionPlan}.
   * @return the generated ID
   */
  public static String generateExecutionPlanId() {
    return "ExecPlan-" + executionPlanIdGenerator.getAndIncrement();
  }

  /**
   * Generates the ID for {@link edu.snu.vortex.runtime.common.execplan.RuntimeVertex}.
   * @param irVertexId .
   * @return the generated ID
   */
  public static String generateRuntimeVertexId(final String irVertexId) {
    return "RVertex-" + irVertexId;
  }

  /**
   * Generates the ID for {@link edu.snu.vortex.runtime.common.execplan.RuntimeEdge}.
   * @param irEdgeId .
   * @return the generated ID
   */
  public static String generateRuntimeEdgeId(final String irEdgeId) {
    return "REdge-" + irEdgeId;
  }

  /**
   * Generates the ID for {@link edu.snu.vortex.runtime.common.execplan.RuntimeStage}.
   * @return the generated ID
   */
  public static String generateRuntimeStageId() {
    return "RuntimeStage-" + runtimeStageIdGenerator.getAndIncrement();
  }
}
