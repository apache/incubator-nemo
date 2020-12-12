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

import com.google.common.collect.Sets;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.exception.UnknownFailureCauseException;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.edge.executionproperty.MessageIdEdgeProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.IgnoreSchedulingTempDataReceiverProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.MessageIdVertexProperty;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.plan.*;
import org.apache.nemo.runtime.common.state.BlockState;
import org.apache.nemo.runtime.common.state.StageState;
import org.apache.nemo.runtime.common.state.TaskState;
import org.apache.nemo.runtime.master.BlockManagerMaster;
import org.apache.nemo.runtime.master.PlanStateManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Utlity methods regarding schedulers.
 */
public final class BatchSchedulerUtils {
  private static final Logger LOG = LoggerFactory.getLogger(BatchSchedulerUtils.class.getName());

  /**
   * Private constructor for utility class.
   */
  private BatchSchedulerUtils() {
  }


  static Optional<List<Stage>> selectEarliestSchedulableGroup(final List<List<Stage>> sortedScheduleGroups,
                                                              final PlanStateManager planStateManager) {
    if (sortedScheduleGroups == null) {
      return Optional.empty();
    }

    return sortedScheduleGroups.stream()
      .filter(scheduleGroup -> scheduleGroup.stream()
        .map(Stage::getId)
        .map(planStateManager::getStageState)
        .anyMatch(state -> state.equals(StageState.State.INCOMPLETE))) // any incomplete stage in the group
      .findFirst(); // selects the one with the smallest scheduling group index.
  }

  static List<Task> selectSchedulableTasks(final PlanStateManager planStateManager,
                                           final BlockManagerMaster blockManagerMaster,
                                           final Stage stageToSchedule) {
    if (stageToSchedule.getPropertyValue(IgnoreSchedulingTempDataReceiverProperty.class).orElse(false)) {
      // Ignore ghost stage.
      for (final String taskId : planStateManager.getTaskAttemptsToSchedule(stageToSchedule.getId())) {
        planStateManager.onTaskStateChanged(taskId, TaskState.State.EXECUTING);
        planStateManager.onTaskStateChanged(taskId, TaskState.State.COMPLETE);
      }

      return Collections.emptyList();
    }

    final List<StageEdge> stageIncomingEdges =
      planStateManager.getPhysicalPlan().getStageDAG().getIncomingEdgesOf(stageToSchedule.getId());
    final List<StageEdge> stageOutgoingEdges =
      planStateManager.getPhysicalPlan().getStageDAG().getOutgoingEdgesOf(stageToSchedule.getId());

    // Create and return tasks.
    final List<Map<String, Readable>> vertexIdToReadables = stageToSchedule.getVertexIdToReadables();

    final List<String> taskIdsToSchedule = planStateManager.getTaskAttemptsToSchedule(stageToSchedule.getId());
    final List<Task> tasks = new ArrayList<>(taskIdsToSchedule.size());
    taskIdsToSchedule.forEach(taskId -> {
      final Set<String> blockIds = BatchSchedulerUtils.getOutputBlockIds(planStateManager, taskId);
      blockManagerMaster.onProducerTaskScheduled(taskId, blockIds);
      final int taskIdx = RuntimeIdManager.getIndexFromTaskId(taskId);
      tasks.add(new Task(
        planStateManager.getPhysicalPlan().getPlanId(),
        taskId,
        stageToSchedule.getExecutionProperties(),
        stageToSchedule.getSerializedIRDAG(),
        stageIncomingEdges,
        stageOutgoingEdges,
        vertexIdToReadables.get(taskIdx)));
    });
    return tasks;
  }

  ////////////////////////////////////////////////////////////////////// Task state change handlers

  /**
   * Action after task execution has been completed.
   * Note this method should not be invoked when the previous state of the task is ON_HOLD.
   *
   * @param executorRegistry the registry for available executors.
   * @param executorId       id of the executor.
   * @param taskId           the ID of the task completed.
   */
  static void onTaskExecutionComplete(final ExecutorRegistry executorRegistry,
                                      final String executorId,
                                      final String taskId) {
    LOG.debug("{} completed in {}", taskId, executorId);
    executorRegistry.updateExecutor(executorId, (executor, state) -> {
      executor.onTaskExecutionComplete(taskId);
      return Pair.of(executor, state);
    });
  }

  /**
   * Get the target edges of dynamic optimization.
   * The edges are annotated with {@link MessageIdEdgeProperty}, which are outgoing edges of
   * parents of the stage put on hold.
   * <p>
   * See {@link org.apache.nemo.compiler.optimizer.pass.compiletime.reshaping.SkewReshapingPass}
   * for setting the target edges of dynamic optimization.
   *
   * @param taskId the task ID that sent stage-level aggregated message for dynamic optimization.
   * @return the edges to optimize.
   */
  static Set<StageEdge> getEdgesToOptimize(final PlanStateManager planStateManager,
                                                   final String taskId) {
    final DAG<Stage, StageEdge> stageDag = planStateManager.getPhysicalPlan().getStageDAG();

    // Get a stage including the given task
    final Stage stagePutOnHold = stageDag.getVertices().stream()
      .filter(stage -> stage.getId().equals(RuntimeIdManager.getStageIdFromTaskId(taskId)))
      .findFirst()
      .orElseThrow(RuntimeException::new);

    // Stage put on hold, i.e. stage with vertex containing MessageAggregatorTransform
    // should have a parent stage whose outgoing edges contain the target edge of dynamic optimization.
    final List<Integer> messageIds = stagePutOnHold.getIRDAG()
      .getVertices()
      .stream()
      .filter(v -> v.getPropertyValue(MessageIdVertexProperty.class).isPresent())
      .map(v -> v.getPropertyValue(MessageIdVertexProperty.class).get())
      .collect(Collectors.toList());
    if (messageIds.size() != 1) {
      throw new IllegalStateException("Must be exactly one vertex with the message id: " + messageIds.toString());
    }
    final int messageId = messageIds.get(0);
    final Set<StageEdge> targetEdges = new HashSet<>();

    // Get edges with identical MessageIdEdgeProperty (except the put on hold stage)
    for (final Stage stage : stageDag.getVertices()) {
      final Set<StageEdge> targetEdgesFound = stageDag.getOutgoingEdgesOf(stage).stream()
        .filter(candidateEdge -> {
          final Optional<HashSet<Integer>> candidateMCId =
            candidateEdge.getPropertyValue(MessageIdEdgeProperty.class);
          return candidateMCId.isPresent() && candidateMCId.get().contains(messageId);
        })
        .collect(Collectors.toSet());
      targetEdges.addAll(targetEdgesFound);
    }

    return targetEdges;
  }

  /**
   * Action for after task execution has failed but it's recoverable.
   *
   * @param executorId   the ID of the executor
   * @param taskId       the ID of the task
   * @param failureCause the cause of failure
   */
  static void onTaskExecutionFailedRecoverable(final PlanStateManager planStateManager,
                                               final BlockManagerMaster blockManagerMaster,
                                               final ExecutorRegistry executorRegistry,
                                               final String executorId,
                                               final String taskId,
                                               final TaskState.RecoverableTaskFailureCause failureCause) {
    LOG.info("{} failed in {} by {}", taskId, executorId, failureCause);
    executorRegistry.updateExecutor(executorId, (executor, state) -> {
      executor.onTaskExecutionFailed(taskId);
      return Pair.of(executor, state);
    });

    switch (failureCause) {
      // Previous task must be re-executed, and incomplete tasks of the belonging stage must be rescheduled.
      case INPUT_READ_FAILURE:
        // TODO #54: Handle remote data fetch failures
      case OUTPUT_WRITE_FAILURE:
        blockManagerMaster.onProducerTaskFailed(taskId);
        break;
      default:
        throw new UnknownFailureCauseException(new Throwable("Unknown cause: " + failureCause));
    }

    retryTasksAndRequiredParents(planStateManager, blockManagerMaster, Collections.singleton(taskId));
  }

  /**
   * Action for after task execution is put on hold.
   *
   * @param executorId the ID of the executor.
   * @param taskId     the ID of the task.
   */
  static Optional<PhysicalPlan> onTaskExecutionOnHold(final PlanStateManager planStateManager,
                                                      final ExecutorRegistry executorRegistry,
                                                      final PlanRewriter planRewriter,
                                                      final String executorId,
                                                      final String taskId) {
    LOG.info("{} put on hold in {}", new Object[]{taskId, executorId});
    executorRegistry.updateExecutor(executorId, (executor, state) -> {
      executor.onTaskExecutionComplete(taskId);
      return Pair.of(executor, state);
    });
    final String stageIdForTaskUponCompletion = RuntimeIdManager.getStageIdFromTaskId(taskId);

    final boolean stageComplete =
      planStateManager.getStageState(stageIdForTaskUponCompletion).equals(StageState.State.COMPLETE);

    final Set<StageEdge> targetEdges = getEdgesToOptimize(planStateManager, taskId);
    if (targetEdges.isEmpty()) {
      throw new RuntimeException("No edges specified for data skew optimization");
    }

    if (stageComplete) {
      return Optional.of(planRewriter.rewrite(getMessageId(targetEdges)));
    } else {
      return Optional.empty();
    }
  }

  /**
   * Process the RuntimePassMessage.
   *
   * @param planStateManager to get the edges for the optimization.
   * @param planRewriter     for rewriting the plan later on.
   * @param taskId           that generated the message.
   * @param data             of the message.
   */
  public static void onRunTimePassMessage(final PlanStateManager planStateManager, final PlanRewriter planRewriter,
                                          final String taskId, final Object data) {
    final Set<StageEdge> targetEdges = BatchSchedulerUtils.getEdgesToOptimize(planStateManager, taskId);
    planRewriter.accumulate(getMessageId(targetEdges), targetEdges, data);
  }

  static int getMessageId(final Set<StageEdge> stageEdges) {
    // Here we simply use findFirst() for now...
    // TODO #345: Simplify insert
    final Set<Integer> messageIds = stageEdges.stream()
      .map(edge -> edge.getExecutionProperties()
        .get(MessageIdEdgeProperty.class)
        .<IllegalArgumentException>orElseThrow(() -> new IllegalArgumentException(edge.getId())))
      .findFirst().<IllegalArgumentException>orElseThrow(IllegalArgumentException::new);
    // Type casting is needed. See: https://stackoverflow.com/a/40865318

    return messageIds.iterator().next();
  }

  ////////////////////////////////////////////////////////////////////// Helper methods

  static void retryTasksAndRequiredParents(final PlanStateManager planStateManager,
                                           final BlockManagerMaster blockManagerMaster,
                                           final Set<String> tasks) {
    final Set<String> requiredParents =
      recursivelyGetParentTasksForLostBlocks(planStateManager, blockManagerMaster, tasks);
    final Set<String> tasksToRetry = Sets.union(tasks, requiredParents);
    LOG.info("Will be retried: {}", tasksToRetry);
    tasksToRetry.forEach(
      taskToReExecute -> planStateManager.onTaskStateChanged(taskToReExecute, TaskState.State.SHOULD_RETRY));
  }

  static Set<String> recursivelyGetParentTasksForLostBlocks(final PlanStateManager planStateManager,
                                                            final BlockManagerMaster blockManagerMaster,
                                                            final Set<String> children) {
    if (children.isEmpty()) {
      return Collections.emptySet();
    }
    final DAG<Stage, StageEdge> stageDAG = planStateManager.getPhysicalPlan().getStageDAG();

    final Map<String, StageEdge> idToIncomingEdges = children.stream()
      .map(RuntimeIdManager::getStageIdFromTaskId)
      .flatMap(stageId -> stageDAG.getIncomingEdgesOf(stageId).stream())
      // Ignore duplicates with the mergeFunction in toMap(_,_,mergeFunction)
      .collect(Collectors.toMap(StageEdge::getId, Function.identity(), (l, r) -> l));

    final Set<String> parentsWithLostBlocks = children.stream()
      .flatMap(child -> getInputBlockIds(planStateManager, child).stream()) // child task id -> parent block ids
      .map(RuntimeIdManager::getWildCardFromBlockId) // parent block id -> parent block wildcard
      .collect(Collectors.toSet()).stream() // remove duplicate wildcards
      .filter(parentBlockWildcard -> // lost block = no matching AVAILABLE block attempt for the wildcard
        blockManagerMaster.getBlockHandlers(parentBlockWildcard, BlockState.State.AVAILABLE).isEmpty())
      .flatMap(lostParentBlockWildcard -> {
        // COMPLETE task attempts of the lostParentBlockWildcard must become SHOULD_RETRY
        final String inEdgeId = RuntimeIdManager.getRuntimeEdgeIdFromBlockId(lostParentBlockWildcard);
        final String parentStageId = idToIncomingEdges.get(inEdgeId).getSrc().getId();
        final int parentTaskIndex = RuntimeIdManager.getTaskIndexFromBlockId(lostParentBlockWildcard);
        return planStateManager.getAllTaskAttemptsOfStage(parentStageId)
          .stream()
          .filter(taskId -> RuntimeIdManager.getStageIdFromTaskId(taskId).equals(parentStageId)
            && RuntimeIdManager.getIndexFromTaskId(taskId) == parentTaskIndex)
          // COMPLETE -> SHOULD_RETRY
          .filter(taskId -> planStateManager.getTaskState(taskId).equals(TaskState.State.COMPLETE));
      })
      .collect(Collectors.toSet());


    // Recursive call
    return Sets.union(parentsWithLostBlocks,
      recursivelyGetParentTasksForLostBlocks(planStateManager, blockManagerMaster, parentsWithLostBlocks));
  }

  static Set<String> getOutputBlockIds(final PlanStateManager planStateManager,
                                       final String taskId) {
    return planStateManager.getPhysicalPlan().getStageDAG()
      .getOutgoingEdgesOf(RuntimeIdManager.getStageIdFromTaskId(taskId))
      .stream()
      .map(stageEdge -> RuntimeIdManager.generateBlockId(stageEdge.getId(), taskId))
      .collect(Collectors.toSet()); // ids of blocks this task will produce
  }

  static Set<String> getInputBlockIds(final PlanStateManager planStateManager,
                                      final String childTaskId) {
    final String stageIdOfChildTask = RuntimeIdManager.getStageIdFromTaskId(childTaskId);
    return planStateManager.getPhysicalPlan().getStageDAG().getIncomingEdgesOf(stageIdOfChildTask)
      .stream()
      .flatMap(inStageEdge -> {
        final Set<String> parentTaskIds = planStateManager.getAllTaskAttemptsOfStage(inStageEdge.getSrc().getId());
        switch (inStageEdge.getDataCommunicationPattern()) {
          case SHUFFLE:
          case BROADCAST:
            // All of the parent stage's tasks
            return parentTaskIds.stream()
              .map(parentTaskId -> RuntimeIdManager.generateBlockId(inStageEdge.getId(), parentTaskId));
          case ONE_TO_ONE:
            // Same-index tasks of the parent stage
            return parentTaskIds.stream()
              .filter(parentTaskId ->
                RuntimeIdManager.getIndexFromTaskId(parentTaskId) == RuntimeIdManager.getIndexFromTaskId(childTaskId))
              .map(parentTaskId -> RuntimeIdManager.generateBlockId(inStageEdge.getId(), parentTaskId));
          default:
            throw new IllegalStateException(inStageEdge.toString());
        }
      })
      .collect(Collectors.toSet());
  }
}
