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

package org.apache.nemo.runtime.master.scheduler;

import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.plan.*;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Scheduler for stream processing. This class follows the structure of
 * {@link StreamingScheduler}, so when a change has to be made on StreamingScheduler, it also means that it should be
 * reflected in this class as well.
 */
public final class StreamingScheduleSimulator extends ScheduleSimulator {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingScheduleSimulator.class.getName());

  @Inject
  private StreamingScheduleSimulator(final SchedulingConstraintRegistry schedulingConstraintRegistry,
                                     final SchedulingPolicy schedulingPolicy,
                                     @Parameter(JobConf.DAGDirectory.class) final String dagDirectory) {
    super(schedulingConstraintRegistry, schedulingPolicy, dagDirectory);
  }

  /**
   * The entrance point of the simulator. Simulate a plan by submitting a plan through this method.
   *
   * @param submittedPhysicalPlan the plan to simulate.
   * @param maxScheduleAttempt    the max number of times this plan/sub-part of the plan should be attempted.
   */
  @Override
  public void schedulePlan(final PhysicalPlan submittedPhysicalPlan, final int maxScheduleAttempt) {
    long startTimeStamp = System.currentTimeMillis();

    // Housekeeping stuff
    getPlanStateManager().updatePlan(submittedPhysicalPlan, maxScheduleAttempt);
    getPlanStateManager().storeJSON("submitted");

    // Prepare tasks
    final List<Stage> allStages = submittedPhysicalPlan.getStageDAG().getTopologicalSort();
    final List<Task> allTasks = allStages.stream().flatMap(stageToSchedule -> {
      // Helper variables for this stage
      final List<StageEdge> stageIncomingEdges =
        submittedPhysicalPlan.getStageDAG().getIncomingEdgesOf(stageToSchedule.getId());
      final List<StageEdge> stageOutgoingEdges =
        submittedPhysicalPlan.getStageDAG().getOutgoingEdgesOf(stageToSchedule.getId());
      final List<Map<String, Readable>> vertexIdToReadables = stageToSchedule.getVertexIdToReadables();
      final List<String> taskIdsToSchedule = getPlanStateManager().getTaskAttemptsToSchedule(stageToSchedule.getId());

      // Create tasks of this stage
      return taskIdsToSchedule.stream().map(taskId -> new Task(
        submittedPhysicalPlan.getPlanId(),
        taskId,
        stageToSchedule.getExecutionProperties(),
        stageToSchedule.getSerializedIRDAG(),
        stageIncomingEdges,
        stageOutgoingEdges,
        vertexIdToReadables.get(RuntimeIdManager.getIndexFromTaskId(taskId))));
    }).collect(Collectors.toList());

    // Schedule everything at once
    getPendingTaskCollectionPointer().setToOverwrite(allTasks);
    getTaskDispatcher().onNewPendingTaskCollectionAvailable();
    LOG.info("Time to assign task to executor: " + (System.currentTimeMillis() - startTimeStamp));
  }

  @Override
  public void updatePlan(final PhysicalPlan newPhysicalPlans) {
    // TODO #227: StreamingScheduler Dynamic Optimization
    throw new UnsupportedOperationException();
  }
}
