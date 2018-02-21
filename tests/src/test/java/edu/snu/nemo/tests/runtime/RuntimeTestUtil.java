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
package edu.snu.nemo.tests.runtime;

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.plan.RuntimeEdge;
import edu.snu.nemo.runtime.common.plan.physical.*;
import edu.snu.nemo.runtime.common.state.BlockState;
import edu.snu.nemo.runtime.common.state.StageState;
import edu.snu.nemo.runtime.common.state.TaskGroupState;
import edu.snu.nemo.runtime.master.JobStateManager;
import edu.snu.nemo.runtime.master.BlockManagerMaster;
import edu.snu.nemo.runtime.master.resource.ExecutorRegistry;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
import edu.snu.nemo.runtime.master.scheduler.PendingTaskGroupQueue;
import edu.snu.nemo.runtime.master.scheduler.Scheduler;
import edu.snu.nemo.runtime.master.scheduler.SchedulingPolicy;
import org.apache.beam.sdk.values.KV;

import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Utility class for runtime unit tests.
 */
public final class RuntimeTestUtil {
  private static ExecutorService completionEventThreadPool;
  private static BlockingDeque<Runnable> eventRunnableQueue;
  private static boolean testComplete;

  public static void initialize() {
    testComplete = false;
    completionEventThreadPool = Executors.newFixedThreadPool(5);

    eventRunnableQueue = new LinkedBlockingDeque<>();

    for (int i = 0; i < 5; i++) {
      completionEventThreadPool.execute(() -> {
        while (!testComplete || !eventRunnableQueue.isEmpty()) {
          try {
            final Runnable event = eventRunnableQueue.takeFirst();
            event.run();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      });
    }
    completionEventThreadPool.shutdown();
  }

  public static void cleanup() {
    testComplete = true;
  }

  /**
   * Sends a stage's completion event to scheduler, with all its task groups marked as complete as well.
   * This replaces executor's task group completion messages for testing purposes.
   * @param jobStateManager for the submitted job.
   * @param scheduler for the submitted job.
   * @param executorRegistry provides executor representers
   * @param physicalStage for which the states should be marked as complete.
   */
  public static void sendStageCompletionEventToScheduler(final JobStateManager jobStateManager,
                                                         final Scheduler scheduler,
                                                         final ExecutorRegistry executorRegistry,
                                                         final PhysicalStage physicalStage,
                                                         final int attemptIdx) {
    eventRunnableQueue.add(new Runnable() {
      @Override
      public void run() {
        while (jobStateManager.getStageState(physicalStage.getId()).getStateMachine().getCurrentState()
            == StageState.State.EXECUTING) {
          physicalStage.getTaskGroupIds().forEach(taskGroupId -> {
            if (jobStateManager.getTaskGroupState(taskGroupId).getStateMachine().getCurrentState()
                == TaskGroupState.State.EXECUTING) {
              sendTaskGroupStateEventToScheduler(scheduler, executorRegistry, taskGroupId,
                  TaskGroupState.State.COMPLETE, attemptIdx, null);
            }
          });
        }
      }
    });
  }

  /**
   * Sends task group state change event to scheduler.
   * This replaces executor's task group completion messages for testing purposes.
   * @param scheduler for the submitted job.
   * @param executorRegistry provides executor representers
   * @param taskGroupId for the task group to change the state.
   * @param newState for the task group.
   * @param cause in the case of a recoverable failure.
   */
  public static void sendTaskGroupStateEventToScheduler(final Scheduler scheduler,
                                                        final ExecutorRegistry executorRegistry,
                                                        final String taskGroupId,
                                                        final TaskGroupState.State newState,
                                                        final int attemptIdx,
                                                        final TaskGroupState.RecoverableFailureCause cause) {
    ExecutorRepresenter scheduledExecutor;
    do {
      scheduledExecutor = findExecutorForTaskGroup(executorRegistry, taskGroupId);
    } while (scheduledExecutor == null);

    scheduler.onTaskGroupStateChanged(scheduledExecutor.getExecutorId(), taskGroupId,
        newState, attemptIdx, null, cause);
  }

  public static void sendTaskGroupStateEventToScheduler(final Scheduler scheduler,
                                                        final ExecutorRegistry executorRegistry,
                                                        final String taskGroupId,
                                                        final TaskGroupState.State newState,
                                                        final int attemptIdx) {
    sendTaskGroupStateEventToScheduler(scheduler, executorRegistry, taskGroupId, newState, attemptIdx, null);
  }

  public static void mockSchedulerRunner(final PendingTaskGroupQueue pendingTaskGroupQueue,
                                         final SchedulingPolicy schedulingPolicy,
                                         final JobStateManager jobStateManager,
                                         final boolean isPartialSchedule) {
    while (!pendingTaskGroupQueue.isEmpty()) {
      final ScheduledTaskGroup taskGroupToSchedule = pendingTaskGroupQueue.dequeue().get();

      schedulingPolicy.scheduleTaskGroup(taskGroupToSchedule, jobStateManager);

      // Schedule only the first task group.
      if (isPartialSchedule) {
        break;
      }
    }
  }

  /**
   * Sends partition state changes of a stage to PartitionManager.
   * This replaces executor's partition state messages for testing purposes.
   * @param blockManagerMaster used for testing purposes.
   * @param executorRegistry provides executor representers
   * @param stageOutgoingEdges to infer partition IDs.
   * @param physicalStage to change the state.
   * @param newState for the task group.
   */
  public static void sendPartitionStateEventForAStage(final BlockManagerMaster blockManagerMaster,
                                                      final ExecutorRegistry executorRegistry,
                                                      final List<PhysicalStageEdge> stageOutgoingEdges,
                                                      final PhysicalStage physicalStage,
                                                      final BlockState.State newState) {
    eventRunnableQueue.add(new Runnable() {
      @Override
      public void run() {
                // Initialize states for blocks of inter-stage edges
        stageOutgoingEdges.forEach(physicalStageEdge -> {
          final int srcParallelism = physicalStage.getTaskGroupIds().size();
          IntStream.range(0, srcParallelism).forEach(srcTaskIdx -> {
            final String partitionId =
                RuntimeIdGenerator.generateBlockId(physicalStageEdge.getId(), srcTaskIdx);
              sendPartitionStateEventToPartitionManager(blockManagerMaster, executorRegistry, partitionId, newState);
          });
        });

        // Initialize states for blocks of stage internal edges
        physicalStage.getTaskGroupIds().forEach(taskGroupId -> {
          final DAG<Task, RuntimeEdge<Task>> taskGroupInternalDag = physicalStage.getTaskGroupDag();
          taskGroupInternalDag.getVertices().forEach(task -> {
            final List<RuntimeEdge<Task>> internalOutgoingEdges = taskGroupInternalDag.getOutgoingEdgesOf(task);
            internalOutgoingEdges.forEach(taskRuntimeEdge -> {
              final String partitionId =
                  RuntimeIdGenerator.generateBlockId(taskRuntimeEdge.getId(),
                      RuntimeIdGenerator.getIndexFromTaskGroupId(taskGroupId));
              sendPartitionStateEventToPartitionManager(blockManagerMaster, executorRegistry, partitionId, newState);
            });
          });
        });
      }
    });
  }

  /**
   * Sends partition state change event to PartitionManager.
   * This replaces executor's partition state messages for testing purposes.
   * @param blockManagerMaster used for testing purposes.
   * @param executorRegistry provides executor representers
   * @param partitionId for the partition to change the state.
   * @param newState for the task group.
   */
  public static void sendPartitionStateEventToPartitionManager(final BlockManagerMaster blockManagerMaster,
                                                               final ExecutorRegistry executorRegistry,
                                                               final String partitionId,
                                                               final BlockState.State newState) {
    eventRunnableQueue.add(new Runnable() {
      @Override
      public void run() {
        final Set<String> parentTaskGroupIds = blockManagerMaster.getProducerTaskGroupIds(partitionId);
        if (!parentTaskGroupIds.isEmpty()) {
          parentTaskGroupIds.forEach(taskGroupId -> {
            final ExecutorRepresenter scheduledExecutor = findExecutorForTaskGroup(executorRegistry, taskGroupId);

            if (scheduledExecutor != null) {
              blockManagerMaster.onBlockStateChanged(
                  partitionId, newState, scheduledExecutor.getExecutorId());
            }
          });
        }
      }
    });
  }

  /**
   * Retrieves the executor to which the given task group was scheduled.
   * @param taskGroupId of the task group to search.
   * @param executorRegistry provides executor representers
   * @return the {@link ExecutorRepresenter} of the executor the task group was scheduled to.
   */
  private static ExecutorRepresenter findExecutorForTaskGroup(final ExecutorRegistry executorRegistry,
                                                              final String taskGroupId) {
    for (final String executorId : executorRegistry.getRunningExecutorIds()) {
      final ExecutorRepresenter executor = executorRegistry.getRunningExecutorRepresenter(executorId);
      if (executor.getRunningTaskGroups().contains(taskGroupId)
          || executor.getCompleteTaskGroups().contains(taskGroupId)) {
        return executor;
      }
    }
    return null;
  }

  /**
   * Gets a list of integer pair elements in range.
   * @param start value of the range (inclusive).
   * @param end   value of the range (exclusive).
   * @return the list of elements.
   */
  public static List getRangedNumList(final int start,
                                               final int end) {
    final List numList = new ArrayList<>(end - start);
    IntStream.range(start, end).forEach(number -> numList.add(KV.of(number, number)));
    return numList;
  }

  /**
   * Flattens a nested list of elements.
   *
   * @param listOfList to flattens.
   * @return the flattened list of elements.
   */
  public static List flatten(final List<List> listOfList) {
    return listOfList.stream().flatMap(list -> ((List<Object>) list).stream()).collect(Collectors.toList());
  }
}
