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
package edu.snu.vortex.runtime.master;

import edu.snu.vortex.runtime.common.plan.logical.*;
import edu.snu.vortex.runtime.common.plan.physical.*;
import edu.snu.vortex.utils.dag.*;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Runtime Master is the central controller of Runtime.
 * Compiler submits an {@link ExecutionPlan} to Runtime Master to execute a job.
 * Runtime Master handles:
 *    a) Physical conversion of a job's DAG into a physical plan.
 *    b) Scheduling the job with {@link edu.snu.vortex.runtime.master.scheduler.Scheduler}.
 *    c) (Please list others done by Runtime Master as features are added).
 */
public final class RuntimeMaster {
  private static final Logger LOG = Logger.getLogger(RuntimeMaster.class.getName());
  // TODO #93: Implement Batch Scheduler
  // private final Scheduler scheduler;

  public RuntimeMaster() {
    // TODO #93: Implement Batch Scheduler
    // this.scheduler = new Scheduler(RuntimeAttribute.Batch);
  }

  /**
   * Submits the {@link ExecutionPlan} to Runtime.
   * @param executionPlan to execute.
   */
  public void execute(final ExecutionPlan executionPlan) {
    final PhysicalPlan physicalPlan = generatePhysicalPlan(executionPlan);
    // TODO #93: Implement Batch Scheduler
    // scheduler.scheduleJob(physicalPlan);
    try {
      new SimpleRuntime().executePhysicalPlan(physicalPlan);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Generates the {@link PhysicalPlan} to be executed.
   * @param executionPlan that should be converted to a physical plan
   * @return {@link PhysicalPlan} to execute.
   */
  private PhysicalPlan generatePhysicalPlan(final ExecutionPlan executionPlan) {
    final DAG<Stage, StageEdge> logicalDAG = executionPlan.getRuntimeStageDAG();
    LOG.log(Level.INFO, "##### Logical DAG #####");
    LOG.log(Level.INFO, logicalDAG.toString());

    final PhysicalPlan physicalPlan = new PhysicalPlan(executionPlan.getId(),
        logicalDAG.convert(new PhysicalDAGGenerator()));
    LOG.log(Level.INFO, "##### Physical DAG #####");
    LOG.log(Level.INFO, physicalPlan.toString());
    return physicalPlan;
  }
}
