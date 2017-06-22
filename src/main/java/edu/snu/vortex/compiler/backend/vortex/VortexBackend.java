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
package edu.snu.vortex.compiler.backend.vortex;

import edu.snu.vortex.compiler.backend.Backend;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.plan.logical.ExecutionPlan;
import edu.snu.vortex.runtime.common.plan.logical.LogicalDAGGenerator;
import edu.snu.vortex.common.dag.DAG;

/**
 * Backend component for Vortex Runtime.
 */
public final class VortexBackend implements Backend<ExecutionPlan> {
  /**
   * Constructor.
   */
  public VortexBackend() {
  }

  /**
   * Compiles an IR DAG into an {@link ExecutionPlan} to be submitted to Runtime.
   * The IR DAG is converted to an execution plan encapsulating a logical DAG using {@link LogicalDAGGenerator}.
   * @param irDAG to compile.
   * @return the execution plan to be submitted to Runtime.
   * @throws Exception any exception occurred during the compilation.
   */
  public ExecutionPlan compile(final DAG<IRVertex, IREdge> irDAG) throws Exception {
    final ExecutionPlan executionPlan =
        new ExecutionPlan(RuntimeIdGenerator.generateExecutionPlanId(), irDAG.convert(new LogicalDAGGenerator()));
    return executionPlan;
  }
}
