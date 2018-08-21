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
import edu.snu.nemo.common.ir.edge.executionproperty.MetricCollectionProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.IgnoreSchedulingTempDataReceiverProperty;
import edu.snu.nemo.runtime.common.RuntimeIdManager;
import edu.snu.nemo.runtime.common.eventhandler.DynamicOptimizationEvent;
import edu.snu.nemo.runtime.common.plan.*;
import edu.snu.nemo.runtime.common.state.TaskState;
import edu.snu.nemo.runtime.master.PlanAppender;
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
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

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
  private final PlanStateManager planStateManager;

  /**
   * Other necessary components of this {@link edu.snu.nemo.runtime.master.RuntimeMaster}.
   */
  private final BlockManagerMaster blockManagerMaster;
  private final PubSubEventHandlerWrapper pubSubEventHandlerWrapper;

  /**
   * The below variables depend on the submitted plan to execute.
   */
  private List<List<Stage>> sortedScheduleGroups;
  private List<DynOptDataHandler> dynOptDataHandlers;

  @Inject
  private BatchScheduler(final TaskDispatcher taskDispatcher,
                         final PendingTaskCollectionPointer pendingTaskCollectionPointer,
                         final BlockManagerMaster blockManagerMaster,
                         final PubSubEventHandlerWrapper pubSubEventHandlerWrapper,
                         final UpdatePhysicalPlanEventHandler updatePhysicalPlanEventHandler,
                         final ExecutorRegistry executorRegistry,
                         final PlanStateManager planStateManager) {
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
    this.planStateManager = planStateManager;
    this.dynOptDataHandlers = new ArrayList<>();
    dynOptDataHandlers.add(new DataSkewDynOptDataHandler());
  }

  /**
   * Schedules a given plan.
   * If multiple physical plans are submitted, they will be appended and handled as a single plan.
   * TODO #182: Consider reshaping in run-time optimization. At now, we only consider plan appending.
   *
   * @param submittedPhysicalPlan the physical plan to schedule.
   * @param maxScheduleAttempt    the max number of times this plan/sub-part of the plan should be attempted.
   */
  @Override
  public void schedulePlan(final PhysicalPlan submittedPhysicalPlan,
                           final int maxScheduleAttempt) {
    LOG.info("Plan to schedule: {}", submittedPhysicalPlan.getPlanId());

    if (!planStateManager.isInitialized()) {
      // First scheduling.
      taskDispatcher.run();
      updatePlan(submittedPhysicalPlan, maxScheduleAttempt);
      planStateManager.storeJSON("submitted");
    } else {
      // Append the submitted plan to the original plan.
      final PhysicalPlan appendedPlan =
          PlanAppender.appendPlan(planStateManager.getPhysicalPlan(), submittedPhysicalPlan);
      updatePlan(appendedPlan, maxScheduleAttempt);
      planStateManager.storeJSON("appended");
    }

    doSchedule();
  }

  @Override
  public void updatePlan(final PhysicalPlan newPhysicalPlan) {
    // update the physical plan in the scheduler.
    // NOTE: what's already been executed is not modified in the new physical plan.
    // TODO #182: Consider reshaping in run-time optimization. At now, we only consider plan appending.
    updatePlan(newPhysicalPlan, planStateManager.getMaxScheduleAttempt());
  }

  /**
   * Update the physical plan in the scheduler.
   *
   * @param newPhysicalPlan    the new physical plan to update.
   * @param maxScheduleAttempt the maximum number of task scheduling attempt.
   */
  private void updatePlan(final PhysicalPlan newPhysicalPlan,
                          final int maxScheduleAttempt) {
    planStateManager.updatePlan(newPhysicalPlan, maxScheduleAttempt);
    this.sortedScheduleGroups = newPhysicalPlan.getStageDAG().getVertices().stream()
        .collect(Collectors.groupingBy(Stage::getScheduleGroup))
        .entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .map(Map.Entry::getValue)
        .collect(Collectors.toList());
  }

  /**
   * Handles task state transition notifications sent from executors.
   * Note that we can receive notifications for previous task attempts, due to the nature of asynchronous events.
   * We ignore such late-arriving notifications, and only handle notifications for the current task attempt.
   *
   * @param executorId       the id of the executor where the message was sent from.
   * @param taskId           whose state has changed
   * @param taskAttemptIndex of the task whose state has changed
   * @param newState         the state to change to
   * @param vertexPutOnHold  the ID of vertex that is put on hold. It is null otherwise.
   */
  public void onTaskStateReportFromExecutor(final String executorId,
                                            final String taskId,
                                            final int taskAttemptIndex,
                                            final TaskState.State newState,
                                            @Nullable final String vertexPutOnHold,
                                            final TaskState.RecoverableTaskFailureCause failureCause) {
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
        throw new RuntimeException("The states READY/EXECUTING cannot occur at this point");
      default:
        throw new UnknownExecutionStateException(new Exception("This TaskState is unknown: " + newState));
    }

    // Invoke doSchedule()
    switch (newState) {
      case COMPLETE:
      case ON_HOLD:
        // If the stage has completed
        final String stageIdForTaskUponCompletion = RuntimeIdManager.getStageIdFromTaskId(taskId);
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
   * <p>
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
          .map(RuntimeIdManager::getStageIdFromTaskId)
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
      final Set<String> blockIds = planStateManager.getPhysicalPlan().getStageDAG()
          .getOutgoingEdgesOf(RuntimeIdManager.getStageIdFromTaskId(taskId))
          .stream()
          .map(stageEdge -> RuntimeIdManager.generateBlockId(stageEdge.getId(), taskId))
          .collect(Collectors.toSet()); // ids of blocks this task will produce
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
   * @param executorId id of the executor.
   * @param taskId     the ID of the task completed.
   */
  private void onTaskExecutionComplete(final String executorId,
                                       final String taskId) {
    LOG.debug("{} completed in {}", taskId, executorId);
    executorRegistry.updateExecutor(executorId, (executor, state) -> {
      executor.onTaskExecutionComplete(taskId);
      return Pair.of(executor, state);
    });
  }

  /**
   * @param taskId the metric collected task ID.
   * @return the edge to optimize.
   */
  private StageEdge getEdgeToOptimize(final String taskId) {
    // Get a stage including the given task
    final Stage stagePutOnHold = planStateManager.getPhysicalPlan().getStageDAG().getVertices().stream()
      .filter(stage -> stage.getId().equals(RuntimeIdManager.getStageIdFromTaskId(taskId)))
      .findFirst()
      .orElseThrow(() -> new RuntimeException());

    // Get outgoing edges of that stage with MetricCollectionProperty
    List<StageEdge> stageEdges = planStateManager.getPhysicalPlan().getStageDAG().getOutgoingEdgesOf(stagePutOnHold);
    for (StageEdge edge : stageEdges) {
      if (edge.getExecutionProperties().containsKey(MetricCollectionProperty.class)) {
        return edge;
      }
    }

    return null;
  }

  /**
   * Action for after task execution is put on hold.
   *
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
    final String stageIdForTaskUponCompletion = RuntimeIdManager.getStageIdFromTaskId(taskId);

    final boolean stageComplete =
        planStateManager.getStageState(stageIdForTaskUponCompletion).equals(StageState.State.COMPLETE);

    final StageEdge targetEdge = getEdgeToOptimize(taskId);
    if (targetEdge == null) {
      throw new RuntimeException("No edges specified for data skew optimization");
    }

    if (stageComplete) {
      final DynOptDataHandler dynOptDataHandler = dynOptDataHandlers.stream()
          .filter(dataHandler -> dataHandler instanceof DataSkewDynOptDataHandler)
          .findFirst().orElseThrow(() -> new RuntimeException("DataSkewDynOptDataHandler is not registered!"));
      pubSubEventHandlerWrapper.getPubSubEventHandler()
          .onNext(new DynamicOptimizationEvent(planStateManager.getPhysicalPlan(), dynOptDataHandler.getDynOptData(),
              taskId, executorId, targetEdge));
    }
  }

  /**
   * Action for after task execution has failed but it's recoverable.
   *
   * @param executorId   the ID of the executor
   * @param taskId       the ID of the task
   * @param failureCause the cause of failure
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

    final Set<String> parentsWithLostBlocks = children.stream()
        .flatMap(child -> getParentTasks(child).stream())
        .filter(parent -> {
          final CompletableFuture<String> locationFuture =
            blockManagerMaster.getBlockLocationHandler(parent).getLocationFuture();
          return locationFuture.isCompletedExceptionally() || locationFuture.isCancelled();
        })
        .collect(Collectors.toSet());

    // Recursive call
    return Sets.union(parentsWithLostBlocks, recursivelyGetParentTasksForLostBlocks(parentsWithLostBlocks));
  }

  private Set<String> getParentTasks(final String childTaskId) {

    final String stageIdOfChildTask = RuntimeIdManager.getStageIdFromTaskId(childTaskId);
    return planStateManager.getPhysicalPlan().getStageDAG().getIncomingEdgesOf(stageIdOfChildTask)
        .stream()
        .flatMap(inStageEdge -> {
          final String parentStageId = inStageEdge.getSrc().getId();
          final Set<String> tasksOfParentStage = planStateManager.getAllTaskAttemptsOfStage(parentStageId);

          switch (inStageEdge.getDataCommunicationPattern()) {
            case Shuffle:
            case BroadCast:
              // All of the parent stage's tasks
              return tasksOfParentStage.stream();
            case OneToOne:
              // Same-index tasks of the parent stage
              return tasksOfParentStage.stream().filter(task ->
                  RuntimeIdManager.getIndexFromTaskId(task) == RuntimeIdManager.getIndexFromTaskId(childTaskId));
            default:
              throw new IllegalStateException(inStageEdge.toString());
          }
        })
        .collect(Collectors.toSet());
  }

  /**
   * Update the data for dynamic optimization.
   *
   * @param dynOptData the data to update.
   */
  public void updateDynOptData(final Object dynOptData) {
    final DynOptDataHandler dynOptDataHandler = dynOptDataHandlers.stream()
        .filter(dataHandler -> dataHandler instanceof DataSkewDynOptDataHandler)
        .findFirst().orElseThrow(() -> new RuntimeException("DataSkewDynOptDataHandler is not registered!"));
    dynOptDataHandler.updateDynOptData(dynOptData);
  }
}
