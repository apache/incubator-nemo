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
package org.apache.nemo.compiler.backend.nemo;

import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.compiler.optimizer.NemoOptimizer;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.common.plan.PlanRewriteEvent;

import javax.inject.Inject;
import java.util.Optional;

/**
 * Rewriting the plan.
 */
public class NemoPlanRewriter {
  private final NemoOptimizer nemoOptimizer;
  private final NemoBackend nemoBackend;

  @Inject
  public NemoPlanRewriter(final NemoOptimizer nemoOptimizer,
                          final NemoBackend nemoBackend) {
    this.nemoOptimizer = nemoOptimizer;
    this.nemoBackend = nemoBackend
  }

  Optional<PhysicalPlan> rewrite(final PhysicalPlan current, final PlanRewriteEvent<?> planRewriteEvent) {
    final IRDAG currentIRDAG = nemoOptimizer.current();
    final IRDAG newIRDAG = nemoOptimizer.optimizeAtRunTime(convert(planRewriteEvent));

    final boolean diff = computeDiff(currentIRDAG, newIRDAG);
    if (diff) {
      final PhysicalPlan newPlan = nemoBackend.compile(newIRDAG);
      return Optional.of(newPlan);
    } else {
      return Optional.empty();
    }
  }
}
