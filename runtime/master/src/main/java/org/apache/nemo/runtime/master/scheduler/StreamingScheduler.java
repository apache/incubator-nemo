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

import com.google.common.collect.Lists;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.exception.UnknownExecutionStateException;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.common.ir.edge.Stage;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.Task;
import org.apache.nemo.common.TaskState;
import org.apache.nemo.runtime.master.*;
import org.apache.reef.annotations.audience.DriverSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.util.*;
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
  private final PipeIndexMaster pipeIndexMaster;
  private final TransferIndexMaster transferIndexMaster;
  private final TaskOffloadingManager taskOffloadingManager;
  private final PairStageTaskManager pairStageTaskManager;
  private final TaskScheduledMapMaster taskScheduledMapMaster;

  @Inject
  StreamingScheduler(final TaskDispatcher taskDispatcher,
                     final PendingTaskCollectionPointer pendingTaskCollectionPointer,
                     final ExecutorRegistry executorRegistry,
                     final PlanStateManager planStateManager,
                     final PipeManagerMaster pipeManagerMaster,
                     final PipeIndexMaster pipeIndexMaster,
                     final TransferIndexMaster transferIndexMaster,
                     final PairStageTaskManager pairStageTaskManager,
                     final TaskScheduledMapMaster taskScheduledMapMaster,
                     final TaskOffloadingManager taskOffloadingManager) {
    this.taskDispatcher = taskDispatcher;
    this.pendingTaskCollectionPointer = pendingTaskCollectionPointer;
    this.executorRegistry = executorRegistry;
    this.planStateManager = planStateManager;
    this.pipeManagerMaster = pipeManagerMaster;
    this.pipeIndexMaster = pipeIndexMaster;
    this.transferIndexMaster = transferIndexMaster;
    this.taskOffloadingManager = taskOffloadingManager;
    this.pairStageTaskManager = pairStageTaskManager;
    this.taskScheduledMapMaster = taskScheduledMapMaster;
  }

  @Override
  public void schedulePlan(final PhysicalPlan submittedPhysicalPlan,
                           final int maxScheduleAttempt) {
    // Housekeeping stuff
    taskDispatcher.run();
    LOG.info("Task dispatcher run");
    planStateManager.updatePlan(submittedPhysicalPlan, maxScheduleAttempt);
    planStateManager.storeJSON("submitted");

    // Prepare tasks
    pairStageTaskManager.registerStageDag(submittedPhysicalPlan.getStageDAG());

    // Reverse topological sort
    final List<Stage> allStages = Lists.reverse(submittedPhysicalPlan.getStageDAG().getTopologicalSort());

    final List<Task> allTasks = new ArrayList<>();

    for (final Stage stageToSchedule : allStages) {
      // Helper variables for this stage
      final List<StageEdge> stageIncomingEdges =
        submittedPhysicalPlan.getStageDAG().getIncomingEdgesOf(stageToSchedule.getId());
      final List<StageEdge> stageOutgoingEdges =
        submittedPhysicalPlan.getStageDAG().getOutgoingEdgesOf(stageToSchedule.getId());
      final List<Map<String, Readable>> vertexIdToReadables = stageToSchedule.getVertexIdToReadables();
      final List<String> taskIdsToSchedule = planStateManager.getTaskAttemptsToSchedule(stageToSchedule.getId());


      LOG.info("Task schedule {}", stageToSchedule.getId());

      taskIdsToSchedule.forEach(taskId -> {
        submittedPhysicalPlan.getStageDAG().getRootVertices().forEach(rootStage -> {
          rootStage.getIRDAG().getRootVertices().forEach(rootVertex -> {
              rootStage.getIRDAG().getOutgoingEdgesOf(rootVertex).forEach(edge -> {
                // register root source pipe for offloading
                // THIS IS FAKE TASK SCHEDULED !!
                pipeIndexMaster.onTaskScheduled("Origin", edge.getId(), taskId);
              });
          });
        });

        LOG.info("Stage incoming edges of {}: {}", stageToSchedule.getId(), stageIncomingEdges);
        stageIncomingEdges.forEach(inEdge -> {
          final int srcParallelism = ((StageEdge) inEdge)
            .getSrc().getPropertyValue(ParallelismProperty.class).get();
          LOG.info("Stage incoming edges of parallelism {}: {} / {}", srcParallelism,
            stageToSchedule.getId(), stageIncomingEdges);

          for (int i = 0; i < srcParallelism; i++) {
            final String srcTask =
              RuntimeIdManager.generateTaskId(inEdge.getSrc().getId(), i, 0);
            pipeIndexMaster.onTaskScheduled(srcTask, inEdge.getId(), taskId);
            // pipeManagerMaster.onTaskScheduled(inEdge.getId(), i);
          }
        });
        // stageOutgoingEdges.forEach(outEdge -> pipeManagerMaster.onTaskScheduled(outEdge.getId(), index));
      });

      // Create tasks of this stage
      allTasks.addAll(
        taskIdsToSchedule.stream().map(taskId -> {
          final Task.TaskType taskType = pairStageTaskManager.registerPairTask(stageIncomingEdges,
            stageOutgoingEdges, taskId, stageToSchedule.getIRDAG());

          final Pair<String, String> pairTaskEdgeId = pairStageTaskManager.getPairTaskEdgeId(taskId);

          final String stageId = RuntimeIdManager.getStageIdFromTaskId(taskId);
          final Set<String> o2oStages = new HashSet<>();
          getO2ODownstreams(stageId, o2oStages);
          getO2OUpstreams(stageId, o2oStages);

          if (pairTaskEdgeId == null) {
            return new Task(
              taskId,
              stageToSchedule.getExecutionProperties(),
              stageToSchedule.getIRDAG(),
              stageIncomingEdges,
              stageOutgoingEdges,
              vertexIdToReadables.get(RuntimeIdManager.getIndexFromTaskId(taskId)),
              null,
              null,
              taskType,
              o2oStages);
          } else {
            return new Task(
              taskId,
              stageToSchedule.getExecutionProperties(),
              stageToSchedule.getIRDAG(),
              stageIncomingEdges,
              stageOutgoingEdges,
              vertexIdToReadables.get(RuntimeIdManager.getIndexFromTaskId(taskId)),
              pairTaskEdgeId.left(),
              pairTaskEdgeId.right(),
              taskType,
              o2oStages);
          }
        })
          .collect(Collectors.toList()));

    }

    // Schedule everything at once
    LOG.info("All tasks: {}", allTasks);

    // Add pending tasks
    taskScheduledMapMaster.tasksToBeScheduled(allTasks);

    try {
      Thread.sleep(6000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    final List<Task> lambdaTasks = allTasks.stream()
      .filter(task -> task.isTransientTask()).collect(Collectors.toList());
    final List<Task> vmTasks = allTasks.stream()
      .filter(task -> !task.isTransientTask()).collect(Collectors.toList());

    LOG.info("VM tasks: {}", vmTasks);
    LOG.info("Lambda tasks: {}", vmTasks);

    pendingTaskCollectionPointer.addTasks(vmTasks);
    pendingTaskCollectionPointer.addTasks(lambdaTasks);
    taskDispatcher.onNewPendingTaskCollectionAvailable();
  }

  private void getO2ODownstreams(final String stageId, final Set<String> l) {
    final List<String> outgoing = planStateManager.getPhysicalPlan().getStageDAG().getOutgoingEdgesOf(stageId)
      .stream().filter(edge -> edge.getDataCommunicationPattern()
        .equals(CommunicationPatternProperty.Value.OneToOne) ||
        edge.getDataCommunicationPattern().equals(CommunicationPatternProperty.Value.TransientOneToOne))
      .map(edge -> edge.getDst().getId())
      .collect(Collectors.toList());

    outgoing.forEach(edge -> {
      if (!l.contains(edge)) {
        l.add(edge);
        getO2ODownstreams(edge, l);
      }
    });
  }

  private void getO2OUpstreams(final String stageId, final Set<String> l) {
    final List<String> incoming = planStateManager.getPhysicalPlan().getStageDAG().getIncomingEdgesOf(stageId)
      .stream().filter(edge -> edge.getDataCommunicationPattern()
        .equals(CommunicationPatternProperty.Value.OneToOne) ||
        edge.getDataCommunicationPattern().equals(CommunicationPatternProperty.Value.TransientOneToOne))
      .map(edge -> edge.getSrc().getId())
      .collect(Collectors.toList());


    incoming.forEach(stage -> {
      if (!l.contains(stage)) {
        l.add(stage);
        getO2OUpstreams(stage, l);
      }
    });
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
