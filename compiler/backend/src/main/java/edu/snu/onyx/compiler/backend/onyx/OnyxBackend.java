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
package edu.snu.onyx.compiler.backend.onyx;

import edu.snu.onyx.common.dag.DAG;
import edu.snu.onyx.compiler.backend.Backend;
import edu.snu.onyx.common.ir.edge.IREdge;
import edu.snu.onyx.common.ir.vertex.IRVertex;
import edu.snu.onyx.runtime.common.RuntimeIdGenerator;
import edu.snu.onyx.runtime.common.plan.physical.PhysicalPlan;
import edu.snu.onyx.runtime.common.plan.physical.PhysicalPlanGenerator;
import edu.snu.onyx.runtime.common.plan.physical.PhysicalStage;
import edu.snu.onyx.runtime.common.plan.physical.PhysicalStageEdge;
import org.apache.reef.tang.Tang;

/**
 * Backend component for Onyx Runtime.
 */
public final class OnyxBackend implements Backend<PhysicalPlan> {
  /**
   * Constructor.
   */
  public OnyxBackend() {
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
   * @throws Exception any exception occurred during the compilation.
   */
  public PhysicalPlan compile(final DAG<IRVertex, IREdge> irDAG,
                              final PhysicalPlanGenerator physicalPlanGenerator) {
    final DAG<PhysicalStage, PhysicalStageEdge> physicalStageDAG = irDAG.convert(physicalPlanGenerator);
    final PhysicalPlan physicalPlan = new PhysicalPlan(RuntimeIdGenerator.generatePhysicalPlanId(),
        physicalStageDAG, physicalPlanGenerator.getTaskIRVertexMap());
    return physicalPlan;
  }
}
