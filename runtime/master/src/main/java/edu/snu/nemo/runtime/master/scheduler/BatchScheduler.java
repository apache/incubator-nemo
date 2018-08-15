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
package edu.snu.nemo.runtime.master.scheduler;

import com.google.common.collect.Sets;
import edu.snu.nemo.common.Pair;
import edu.snu.nemo.common.eventhandler.PubSubEventHandlerWrapper;
import edu.snu.nemo.common.ir.Readable;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.MetricCollectionProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.eventhandler.DynamicOptimizationEvent;
import edu.snu.nemo.runtime.common.plan.*;
import edu.snu.nemo.runtime.common.state.BlockState;
import edu.snu.nemo.runtime.common.state.TaskState;
import edu.snu.nemo.runtime.master.DataSkewDynOptDataHandler;
import edu.snu.nemo.runtime.master.DynOptDataHandler;
import edu.snu.nemo.runtime.master.eventhandler.UpdatePhysicalPlanEventHandler;
import edu.snu.nemo.common.exception.*;
import edu.snu.nemo.runtime.common.state.StageState;
import edu.snu.nemo.runtime.master.BlockManagerMaster;
import edu.snu.nemo.runtime.master.PlanStateManager;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;

/**
 * (CONCURRENCY) Only a single dedicated thread should use the public methods of this class.
 * (i.e., runtimeMasterThread in RuntimeMaster)
 *
 * BatchScheduler receives a single {@link PhysicalPlan} to execute and schedules the Tasks.
 */
@DriverSide
@NotThreadSafe
public final class BatchScheduler implements Scheduler {
  private static final Logger LOG = LoggerFactory.getLogger(BatchScheduler.class.getName());

  /**
   * Components related to scheduling the given plan.
   */
  private final TaskDispatcher taskDispatcher;
  private final PendingTaskCollectionPointer pendingTaskCollectionPointer;
  private final ExecutorRegistry executorRegistry;

  /**
   * Other necessary components of this {@link edu.snu.nemo.runtime.master.RuntimeMaster}.
   */
  private final BlockManagerMaster blockManagerMaster;
  private final PubSubEventHandlerWrapper pubSubEventHandlerWrapper;

  /**
   * The below variables depend on the submitted plan to execute.
   */
  private PhysicalPlan physicalPlan;
  private PlanStateManager planStateManager;
  private List<List<Stage>> sortedScheduleGroups;
  private List<DynOptDataHandler> dynOptDataHandlers;

  @Inject
  private BatchScheduler(final TaskDispatcher taskDispatcher,
                         final PendingTaskCollectionPointer pendingTaskCollectionPointer,
                         final BlockManagerMaster blockManagerMaster,
                         final PubSubEventHandlerWrapper pubSubEventHandlerWrapper,
                         final UpdatePhysicalPlanEventHandler updatePhysicalPlanEventHandler,
                         final ExecutorRegistry executorRegistry) {
    this.taskDispatcher = taskDispatcher;
    this.pendingTaskCollectionPointer = pendingTaskCollectionPointer;
    this.blockManagerMaster = blockManagerMaster;
    this.pubSubEventHandlerWrapper = pubSubEventHandlerWrapper;
    updatePhysicalPlanEventHandler.setScheduler(this);
    if (pubSubEventHandlerWrapper.getPubSubEventHandler() != null) {
      pubSubEventHandlerWrapper.getPubSubEventHandler()
          .subscribe(updatePhysicalPlanEventHandler.getEventClass(), updatePhysicalPlanEventHandler);
    }
    this.executorRegistry = executorRegistry;
    this.dynOptDataHandlers = new ArrayList<>();
    dynOptDataHandlers.add(new DataSkewDynOptDataHandler());
  }

  /**
   * @param submittedPhysicalPlan the physical plan to schedule.
   * @param submittedPlanStateManager the state manager of the plan.
   */
  @Override
  public void schedulePlan(final PhysicalPlan submittedPhysicalPlan, final PlanStateManager submittedPlanStateManager) {
    LOG.info("Scheduled plan");

    this.physicalPlan = submittedPhysicalPlan;
    this.planStateManager = submittedPlanStateManager;

    taskDispatcher.run(this.planStateManager);
    LOG.info("Plan to schedule: {}", this.physicalPlan.getId());

    this.sortedScheduleGroups = this.physicalPlan.getStageDAG().getVertices().stream()
        .collect(Collectors.groupingBy(Stage::getScheduleGroup))
        .entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .map(Map.Entry::getValue)
        .collect(Collectors.toList());

    doSchedule();
  }

  @Override
  public void updatePlan(final String planId, final PhysicalPlan newPhysicalPlan) {
    // update the physical plan in the scheduler.
    // NOTE: what's already been executed is not modified in the new physical plan.
    this.physicalPlan = newPhysicalPlan;
  }

  /**
   * Handles task state transition notifications sent from executors.
   * Note that we can receive notifications for previous task attempts, due to the nature of asynchronous events.
   * We ignore such late-arriving notifications, and only handle notifications for the current task attempt.
   * @param executorId the id of the executor where the message was sent from.
   * @param taskId whose state has changed
   * @param taskAttemptIndex of the task whose state has changed
   * @param newState the state to change to
   * @param vertexPutOnHold the ID of vertex that is put on hold. It is null otherwise.
   */
  public void onTaskStateReportFromExecutor(final String executorId,
                                            final String taskId,
                                            final int taskAttemptIndex,
                                            final TaskState.State newState,
                                            @Nullable final String vertexPutOnHold,
                                            final TaskState.RecoverableTaskFailureCause failureCause) {
    final int currentTaskAttemptIndex = planStateManager.getTaskAttempt(taskId);

    if (taskAttemptIndex == currentTaskAttemptIndex) {
      // Do change state, as this notification is for the current task attempt.
      planStateManager.onTaskStateChanged(taskId, newState);
      switch (newState) {
        case COMPLETE:
          onTaskExecutionComplete(executorId, taskId);
          break;
        case SHOULD_RETRY:
          // SHOULD_RETRY from an executor means that the task ran into a recoverable failure
          onTaskExecutionFailedRecoverable(executorId, taskId, failureCause);
          break;
        case ON_HOLD:
          onTaskExecutionOnHold(executorId, taskId);
          break;
        case FAILED:
          throw new UnrecoverableFailureException(new Exception(new StringBuffer().append("The plan failed on Task #")
              .append(taskId).append(" in Executor ").append(executorId).toString()));
        case READY:
        case EXECUTING:
          throw new IllegalStateTransitionException(
              new Exception("The states READY/EXECUTING cannot occur at this point"));
        default:
          throw new UnknownExecutionStateException(new Exception("This TaskState is unknown: " + newState));
      }

      // Invoke doSchedule()
      switch (newState) {
        case COMPLETE:
        case ON_HOLD:
          // If the stage has completed
          final String stageIdForTaskUponCompletion = RuntimeIdGenerator.getStageIdFromTaskId(taskId);
          if (planStateManager.getStageState(stageIdForTaskUponCompletion).equals(StageState.State.COMPLETE)) {
            if (!planStateManager.isPlanDone()) {
              doSchedule();
            }
          }
          break;
        case SHOULD_RETRY:
          // Do retry
          doSchedule();
          break;
        default:
          break;
      }

      // Invoke taskDispatcher.onExecutorSlotAvailable()
      switch (newState) {
        // These three states mean that a slot is made available.
        case COMPLETE:
        case ON_HOLD:
        case SHOULD_RETRY:
          taskDispatcher.onExecutorSlotAvailable();
          break;
        default:
          break;
      }
    } else if (taskAttemptIndex < currentTaskAttemptIndex) {
      // Do not change state, as this report is from a previous task attempt.
      // For example, the master can receive a notification that an executor has been removed,
      // and then a notification that the task that was running in the removed executor has been completed.
      // In this case, if we do not consider the attempt number, the state changes from SHOULD_RETRY to COMPLETED,
      // which is illegal.
      LOG.info("{} state change to {} arrived late, we will ignore this.", new Object[]{taskId, newState});
    } else {
      throw new SchedulingException(new Throwable("AttemptIdx for a task cannot be greater than its current index"));
    }
  }

  @Override
  public void onExecutorAdded(final ExecutorRepresenter executorRepresenter) {
    LOG.info("{} added (node: {})", executorRepresenter.getExecutorId(), executorRepresenter.getNodeName());
    executorRegistry.registerExecutor(executorRepresenter);
    taskDispatcher.onExecutorSlotAvailable();
  }

  @Override
  public void onExecutorRemoved(final String executorId) {
    LOG.info("{} removed", executorId);
    blockManagerMaster.removeWorker(executorId);

    // These are tasks that were running at the time of executor removal.
    final Set<String> interruptedTasks = new HashSet<>();
    executorRegistry.updateExecutor(executorId, (executor, state) -> {
      interruptedTasks.addAll(executor.onExecutorFailed());
      return Pair.of(executor, ExecutorRegistry.ExecutorState.FAILED);
    });

    // Retry the interrupted tasks (and required parents)
    retryTasksAndRequiredParents(interruptedTasks);

    // Trigger the scheduling of SHOULD_RETRY tasks in the earliest scheduleGroup
    doSchedule();
  }

  @Override
  public void terminate() {
    this.taskDispatcher.terminate();
    this.executorRegistry.terminate();
  }

  ////////////////////////////////////////////////////////////////////// Key methods for scheduling

  /**
   * The main entry point for task scheduling.
   * This operation can be invoked at any point during job execution, as it is designed to be free of side-effects.
   *
   * These are the reasons why.
   * - We 'reset' {@link PendingTaskCollectionPointer}, and not 'add' new tasks to it
   * - We make {@link TaskDispatcher} dispatch only the tasks that are READY.
   */
  private void doSchedule() {
    final Optional<List<Stage>> earliest = selectEarliestSchedulableGroup();

    if (earliest.isPresent()) {
      // Get schedulable tasks.
      final List<Task> tasksToSchedule = earliest.get().stream()
          .flatMap(stage -> selectSchedulableTasks(stage).stream())
          .collect(Collectors.toList());

      // We prefer (but not guarantee) to schedule the 'receiving' tasks first,
      // assuming that tasks within a ScheduleGroup are connected with 'push' edges.
      Collections.reverse(tasksToSchedule);

      LOG.info("Scheduling some tasks in {}, which are in the same ScheduleGroup", tasksToSchedule.stream()
          .map(Task::getTaskId)
          .map(RuntimeIdGenerator::getStageIdFromTaskId)
          .collect(Collectors.toSet()));

      // Set the pointer to the schedulable tasks.
      pendingTaskCollectionPointer.setToOverwrite(tasksToSchedule);

      // Notify the dispatcher that a new collection is available.
      taskDispatcher.onNewPendingTaskCollectionAvailable();
    } else {
      LOG.info("Skipping this round as no ScheduleGroup is schedulable.");
    }
  }

  private Optional<List<Stage>> selectEarliestSchedulableGroup() {
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

  private List<Task> selectSchedulableTasks(final Stage stageToSchedule) {
    final List<StageEdge> stageIncomingEdges =
        physicalPlan.getStageDAG().getIncomingEdgesOf(stageToSchedule.getId());
    final List<StageEdge> stageOutgoingEdges =
        physicalPlan.getStageDAG().getOutgoingEdgesOf(stageToSchedule.getId());

    final List<String> taskIdsToSchedule = new LinkedList<>();
    for (final String taskId : stageToSchedule.getTaskIds()) {
      final TaskState.State taskState = planStateManager.getTaskState(taskId);

      switch (taskState) {
        // Don't schedule these.
        case COMPLETE:
        case EXECUTING:
        case ON_HOLD:
          break;

        // These are schedulable.
        case SHOULD_RETRY:
          planStateManager.onTaskStateChanged(taskId, TaskState.State.READY);
        case READY:
          taskIdsToSchedule.add(taskId);
          break;

        // This shouldn't happen.
        default:
          throw new SchedulingException(new Throwable("Detected a FAILED Task"));
      }
    }

    // Create and return tasks.
    final List<Map<String, Readable>> vertexIdToReadables = stageToSchedule.getVertexIdToReadables();
    final List<Task> tasks = new ArrayList<>(taskIdsToSchedule.size());
    taskIdsToSchedule.forEach(taskId -> {
      blockManagerMaster.onProducerTaskScheduled(taskId); // Notify the block manager early for push edges.
      final int taskIdx = RuntimeIdGenerator.getIndexFromTaskId(taskId);
      final int attemptIdx = planStateManager.getTaskAttempt(taskId);
      tasks.add(new Task(
          physicalPlan.getId(),
          taskId,
          attemptIdx,
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
   * @param executorId id of the executor.
   * @param taskId the ID of the task completed.
   */
  private void onTaskExecutionComplete(final String executorId,
                                       final String taskId) {
    LOG.debug("{} completed in {}", new Object[]{taskId, executorId});
    executorRegistry.updateExecutor(executorId, (executor, state) -> {
      executor.onTaskExecutionComplete(taskId);
      return Pair.of(executor, state);
    });
  }

  public IREdge getEdgeToOptimize(final String taskId) {
    // Get a stage including the given task
    final Stage stagePutOnHold = physicalPlan.getStageDAG().getVertices().stream()
        .filter(stage -> stage.getTaskIds().contains(taskId)).findFirst().get();

    // Get outgoing edges of that stage with MetricCollectionProperty
    List<StageEdge> stageEdges = physicalPlan.getStageDAG().getOutgoingEdgesOf(stagePutOnHold);
    IREdge targetEdge = null;
    for (StageEdge edge : stageEdges) {
      final IRVertex srcIRVertex = edge.getSrcIRVertex();
      final IRVertex dstIRVertex = edge.getDstIRVertex();
      targetEdge = physicalPlan.getIrDAG().getEdgeBetween(srcIRVertex.getId(), dstIRVertex.getId());
      if (MetricCollectionProperty.Value.DataSkewRuntimePass
          .equals(targetEdge.getPropertyValue(MetricCollectionProperty.class).get())) {
        break;
      }
    }

    return targetEdge;
  }

  /**
   * Action for after task execution is put on hold.
   * @param executorId       the ID of the executor.
   * @param taskId           the ID of the task.
   */
  private void onTaskExecutionOnHold(final String executorId,
                                     final String taskId) {
    LOG.info("{} put on hold in {}", new Object[]{taskId, executorId});
    executorRegistry.updateExecutor(executorId, (executor, state) -> {
      executor.onTaskExecutionComplete(taskId);
      return Pair.of(executor, state);
    });
    final String stageIdForTaskUponCompletion = RuntimeIdGenerator.getStageIdFromTaskId(taskId);

    final boolean stageComplete =
        planStateManager.getStageState(stageIdForTaskUponCompletion).equals(StageState.State.COMPLETE);

    final IREdge targetEdge = getEdgeToOptimize(taskId);
    if (targetEdge == null) {
      throw new RuntimeException("No edges specified for data skew optimization");
    }

    if (stageComplete) {
      final DynOptDataHandler dynOptDataHandler = dynOptDataHandlers.stream()
          .filter(dataHandler -> dataHandler instanceof DataSkewDynOptDataHandler)
          .findFirst().orElseThrow(() -> new RuntimeException("DataSkewDynOptDataHandler is not registered!"));
      pubSubEventHandlerWrapper.getPubSubEventHandler()
          .onNext(new DynamicOptimizationEvent(physicalPlan, dynOptDataHandler.getDynOptData(),
              taskId, executorId, targetEdge));
    }
  }

  /**
   * Action for after task execution has failed but it's recoverable.
   * @param executorId    the ID of the executor
   * @param taskId   the ID of the task
   * @param failureCause  the cause of failure
   */
  private void onTaskExecutionFailedRecoverable(final String executorId,
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

    retryTasksAndRequiredParents(Collections.singleton(taskId));
  }

  ////////////////////////////////////////////////////////////////////// Helper methods

  private void retryTasksAndRequiredParents(final Set<String> tasks) {
    final Set<String> requiredParents = recursivelyGetParentTasksForLostBlocks(tasks);
    final Set<String> tasksToRetry = Sets.union(tasks, requiredParents);
    LOG.info("Will be retried: {}", tasksToRetry);
    tasksToRetry.forEach(
        taskToReExecute -> planStateManager.onTaskStateChanged(taskToReExecute, TaskState.State.SHOULD_RETRY));
  }

  private Set<String> recursivelyGetParentTasksForLostBlocks(final Set<String> children) {
    if (children.isEmpty()) {
      return Collections.emptySet();
    }

    final Set<String> selectedParentTasks = children.stream()
        .flatMap(child -> getParentTasks(child).stream())
        .filter(parent -> blockManagerMaster.getIdsOfBlocksProducedBy(parent).stream()
            .map(blockManagerMaster::getBlockState)
            .anyMatch(blockState -> blockState.equals(BlockState.State.NOT_AVAILABLE)) // If a block is missing
        )
        .collect(Collectors.toSet());

    // Recursive call
    return Sets.union(selectedParentTasks, recursivelyGetParentTasksForLostBlocks(selectedParentTasks));
  }

  private Set<String> getParentTasks(final String childTaskId) {
    final String stageIdOfChildTask = RuntimeIdGenerator.getStageIdFromTaskId(childTaskId);
    return physicalPlan.getStageDAG().getIncomingEdgesOf(stageIdOfChildTask)
        .stream()
        .flatMap(inStageEdge -> {
          final List<String> tasksOfParentStage = inStageEdge.getSrc().getTaskIds();
          switch (inStageEdge.getDataCommunicationPattern()) {
            case Shuffle:
            case BroadCast:
              // All of the parent stage's tasks are parents
              return tasksOfParentStage.stream();
            case OneToOne:
              // Only one of the parent stage's tasks is a parent
              return Stream.of(tasksOfParentStage.get(RuntimeIdGenerator.getIndexFromTaskId(childTaskId)));
            default:
              throw new IllegalStateException(inStageEdge.toString());
          }
        })
        .collect(Collectors.toSet());
  }

  public void updateDynOptData(final Object dynOptData) {
    final DynOptDataHandler dynOptDataHandler = dynOptDataHandlers.stream()
        .filter(dataHandler -> dataHandler instanceof DataSkewDynOptDataHandler)
        .findFirst().orElseThrow(() -> new RuntimeException("DataSkewDynOptDataHandler is not registered!"));
    dynOptDataHandler.updateDynOptData(dynOptData);
  }
}
