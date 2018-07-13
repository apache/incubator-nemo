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
package edu.snu.nemo.compiler.backend.nemo;

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.compiler.backend.Backend;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.plan.PhysicalPlan;
import edu.snu.nemo.runtime.common.plan.PhysicalPlanGenerator;
import edu.snu.nemo.runtime.common.plan.Stage;
import edu.snu.nemo.runtime.common.plan.StageEdge;
import org.apache.reef.tang.Tang;

import javax.inject.Inject;

/**
 * Backend component for Nemo Runtime.
 */
public final class NemoBackend implements Backend<PhysicalPlan> {
  /**
   * Constructor.
   */
  @Inject
  private NemoBackend() {
  }

  /**
   * Compiles an IR DAG into a {@link PhysicalPlan} to be submitted to Runtime.
   * @param irDAG to compile.
   * @return the execution plan to be submitted to Runtime.
   * @throws Exception any exception occurred during the compilation.
   */
  public PhysicalPlan compile(final DAG<IRVertex, IREdge> irDAG) throws Exception {
    final PhysicalPlanGenerator physicalPlanGenerator =
        Tang.Factory.getTang().newInjector().getInstance(PhysicalPlanGenerator.class);
    return compile(irDAG, physicalPlanGenerator);
  }

  /**
   * Compiles an IR DAG into a {@link PhysicalPlan} to be submitted to Runtime.
   * Receives {@link PhysicalPlanGenerator} with configured directory of DAG files.
   * @param irDAG to compile.
   * @param physicalPlanGenerator with custom DAG directory.
   * @return the execution plan to be submitted to Runtime.
   */
  public PhysicalPlan compile(final DAG<IRVertex, IREdge> irDAG,
                              final PhysicalPlanGenerator physicalPlanGenerator) {
    final DAG<Stage, StageEdge> stageDAG = irDAG.convert(physicalPlanGenerator);
    final PhysicalPlan physicalPlan = new PhysicalPlan(RuntimeIdGenerator.generatePhysicalPlanId(), irDAG, stageDAG);
    return physicalPlan;
  }
}
