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

import org.apache.nemo.common.exception.UnknownExecutionStateException;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.common.plan.Stage;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.common.state.TaskState;
import org.apache.nemo.runtime.master.PipeManagerMaster;
import org.apache.nemo.runtime.master.PlanStateManager;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A simple scheduler for streaming workloads.
 * - Keeps track of new executors
 * - Schedules all tasks in the plan at once.
 * - Crashes the system upon any other events (should be fixed in the future)
 * - Never stops running.
 */
@DriverSide
@NotThreadSafe
public final class StreamingScheduler implements Scheduler {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingScheduler.class.getName());

  private final TaskDispatcher taskDispatcher;
  private final PendingTaskCollectionPointer pendingTaskCollectionPointer;
  private final ExecutorRegistry executorRegistry;
  private final PlanStateManager planStateManager;
  private final PipeManagerMaster pipeManagerMaster;

  @Inject
  StreamingScheduler(final TaskDispatcher taskDispatcher,
                     final PendingTaskCollectionPointer pendingTaskCollectionPointer,
                     final ExecutorRegistry executorRegistry,
                     final PlanStateManager planStateManager,
                     final PipeManagerMaster pipeManagerMaster) {
    this.taskDispatcher = taskDispatcher;
    this.pendingTaskCollectionPointer = pendingTaskCollectionPointer;
    this.executorRegistry = executorRegistry;
    this.planStateManager = planStateManager;
    this.pipeManagerMaster = pipeManagerMaster;
  }

  @Override
  public void schedulePlan(final PhysicalPlan submittedPhysicalPlan,
                           final int maxScheduleAttempt) {
    // Housekeeping stuff
    taskDispatcher.run();
    planStateManager.updatePlan(submittedPhysicalPlan, maxScheduleAttempt);
    planStateManager.storeJSON("submitted");

    // Prepare tasks
    final List<Stage> allStages = submittedPhysicalPlan.getStageDAG().getTopologicalSort();
    final List<Task> allTasks = allStages.stream().flatMap(stageToSchedule -> {
      // Helper variables for this stage
      final List<StageEdge> stageIncomingEdges =
        submittedPhysicalPlan.getStageDAG().getIncomingEdgesOf(stageToSchedule.getId());
      final List<StageEdge> stageOutgoingEdges =
        submittedPhysicalPlan.getStageDAG().getOutgoingEdgesOf(stageToSchedule.getId());
      final List<Map<String, Readable>> vertexIdToReadables = stageToSchedule.getVertexIdToReadables();
      final List<String> taskIdsToSchedule = planStateManager.getTaskAttemptsToSchedule(stageToSchedule.getId());

      taskIdsToSchedule.forEach(taskId -> {
        final int index = RuntimeIdManager.getIndexFromTaskId(taskId);
        stageOutgoingEdges.forEach(outEdge -> pipeManagerMaster.onTaskScheduled(outEdge.getId(), index));
      });

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
    pendingTaskCollectionPointer.setToOverwrite(allTasks);
    taskDispatcher.onNewPendingTaskCollectionAvailable();
  }

  @Override
  public void updatePlan(final PhysicalPlan newPhysicalPlan) {
    // TODO #227: StreamingScheduler Dynamic Optimization
    throw new UnsupportedOperationException();
  }

  @Override
  public void onTaskStateReportFromExecutor(final String executorId,
                                            final String taskId,
                                            final int taskAttemptIndex,
                                            final TaskState.State newState,
                                            @Nullable final String vertexPutOnHold,
                                            final TaskState.RecoverableTaskFailureCause failureCause) {
    planStateManager.onTaskStateChanged(taskId, newState);

    switch (newState) {
      case COMPLETE:
        // Do nothing.
        break;
      case ON_HOLD:
      case FAILED:
      case SHOULD_RETRY:
        // TODO #226: StreamingScheduler Fault Tolerance
        throw new UnsupportedOperationException();
      case READY:
      case EXECUTING:
        throw new RuntimeException("The states READY/EXECUTING cannot occur at this point");
      default:
        throw new UnknownExecutionStateException(new Exception("This TaskState is unknown: " + newState));
    }
  }

  @Override
  public void onSpeculativeExecutionCheck() {
    // TODO #228: StreamingScheduler Speculative Execution
    throw new UnsupportedOperationException();
  }

  @Override
  public void onWorkStealingCheck() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void onExecutorAdded(final ExecutorRepresenter executorRepresenter) {
    LOG.info("{} added (node: {})", executorRepresenter.getExecutorId(), executorRepresenter.getNodeName());
    taskDispatcher.onExecutorSlotAvailable();
    executorRegistry.registerExecutor(executorRepresenter);
  }

  @Override
  public void onExecutorRemoved(final String executorId) {
    // TODO #226: StreamingScheduler Fault Tolerance
    throw new UnsupportedOperationException();
  }

  @Override
  public void terminate() {
    this.taskDispatcher.terminate();
    this.executorRegistry.terminate();
  }
}
