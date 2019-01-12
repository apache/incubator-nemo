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

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.compiler.backend.Backend;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.common.plan.PhysicalPlanGenerator;
import org.apache.nemo.runtime.common.plan.Stage;
import org.apache.nemo.runtime.common.plan.StageEdge;

import javax.inject.Inject;

/**
 * Backend component for Nemo Runtime.
 */
public final class NemoBackend implements Backend<PhysicalPlan> {

  private final PhysicalPlanGenerator physicalPlanGenerator;

  /**
   * Constructor.
   * @param physicalPlanGenerator physical plan generator.
   */
  @Inject
  private NemoBackend(final PhysicalPlanGenerator physicalPlanGenerator) {
    this.physicalPlanGenerator = physicalPlanGenerator;
  }

  /**
   * Compiles an IR DAG into a {@link PhysicalPlan} to be submitted to Runtime.
   *
   * @param irDAG the IR DAG to compile.
   * @return the execution plan to be submitted to Runtime.
   */
  public PhysicalPlan compile(final DAG<IRVertex, IREdge> irDAG) {

    final DAG<Stage, StageEdge> stageDAG = physicalPlanGenerator.apply(irDAG);
    return new PhysicalPlan(RuntimeIdManager.generatePhysicalPlanId(), stageDAG);
  }
}
