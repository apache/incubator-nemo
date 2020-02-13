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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.Util;
import org.apache.nemo.common.exception.IllegalMessageException;
import org.apache.nemo.common.exception.SimulationException;
import org.apache.nemo.common.exception.UnknownExecutionStateException;
import org.apache.nemo.common.exception.UnrecoverableFailureException;
import org.apache.nemo.common.ir.executionproperty.ResourceSpecification;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.ClientRPC;
import org.apache.nemo.runtime.common.message.MessageSender;
import org.apache.nemo.runtime.common.message.MessageUtils;
import org.apache.nemo.runtime.common.metric.JobMetric;
import org.apache.nemo.runtime.common.metric.Metric;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.common.plan.PlanRewriter;
import org.apache.nemo.runtime.common.plan.Stage;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.common.state.StageState;
import org.apache.nemo.runtime.common.state.TaskState;
import org.apache.nemo.runtime.master.BlockManagerMaster;
import org.apache.nemo.runtime.master.PlanAppender;
import org.apache.nemo.runtime.master.PlanStateManager;
import org.apache.nemo.runtime.master.metric.MetricStore;
import org.apache.nemo.runtime.master.resource.DefaultExecutorRepresenter;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.EvaluatorDescriptor;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Scheduler for simulating an execution, not controlled by the runtime master.
 */
@DriverSide
@NotThreadSafe
public final class SimulationScheduler implements Scheduler {
  private static final Logger LOG = LoggerFactory.getLogger(SimulationScheduler.class.getName());

  /**
   * Run-time optimizations.
   */
  private final PlanRewriter planRewriter;
  private final ClientRPC clientRPC;

  /**
   * Components related to scheduling the given plan.
   */
  private final TaskDispatcher taskDispatcher;
  private final PendingTaskCollectionPointer pendingTaskCollectionPointer;
  private final ExecutorRegistry executorRegistry;
  private final PlanStateManager planStateManager;
  private final ExecutorService serializationExecutorService; // Executor service for scheduling message serialization.
  private final BlockManagerMaster blockManagerMaster;
  private final MetricStore actualMetricStore;
  private final MetricStore metricStore;
  private final CountDownLatch metricCountDownLatch;

  private final Map<String, SimulatedTaskExecutor> simulatedTaskExecutorMap;

  /**
   * The below variables depend on the submitted plan to execute.
   */
  private List<List<Stage>> sortedScheduleGroups;

  @Inject
  private SimulationScheduler(final PlanRewriter planRewriter,
                              final SchedulingConstraintRegistry schedulingConstraintRegistry,
                              final SchedulingPolicy schedulingPolicy,
                              final BlockManagerMaster blockManagerMaster,
                              final ClientRPC clientRPC,
                              @Parameter(JobConf.ExecutorJSONContents.class) final String resourceSpecificationString,
                              @Parameter(JobConf.ScheduleSerThread.class) final int scheduleSerThread,
                              @Parameter(JobConf.DAGDirectory.class) final String dagDirectory) {
    this.planRewriter = planRewriter;
    this.blockManagerMaster = blockManagerMaster;
    this.pendingTaskCollectionPointer = PendingTaskCollectionPointer.newInstance();
    this.executorRegistry = ExecutorRegistry.newInstance();
    this.planStateManager = PlanStateManager.newInstance(dagDirectory);
    this.taskDispatcher = TaskDispatcher.newInstance(schedulingConstraintRegistry, schedulingPolicy,
      pendingTaskCollectionPointer, executorRegistry, planStateManager);
    this.serializationExecutorService = Executors.newFixedThreadPool(scheduleSerThread);
    this.clientRPC = clientRPC;
    this.actualMetricStore = MetricStore.getStore();
    this.metricStore = getSimulationMetricStore();
    this.simulatedTaskExecutorMap = new HashMap<>();

    // Simulate launch of executors.
    final List<Pair<Integer, ResourceSpecification>> resourceSpecs =
      Util.parseResourceSpecificationString(resourceSpecificationString);
    // Role of ActiveContextHandler + RuntimeMaster.onExecuterLaunched.
    final AtomicInteger executorIdGenerator = new AtomicInteger(0);
    final AtomicInteger resourceRequestCount = new AtomicInteger(0);
    resourceSpecs.forEach(p -> {
      for (int i = 0; i < p.left(); i++) {
        resourceRequestCount.getAndIncrement();
        final ActiveContext ac = new SimulationEvaluatorActiveContext(executorIdGenerator.getAndIncrement());
        this.onExecutorAdded(new DefaultExecutorRepresenter(ac.getId(), p.right(),
          new SimulationMessageSender(ac.getId(), this), ac, serializationExecutorService, ac.getId()));
      }
    });
    this.metricCountDownLatch = new CountDownLatch(resourceRequestCount.get());
  }

  /**
   * Instance holder for metric store.
   */
  private static class MetricStoreInstanceHolder {
    private static final MetricStore INSTANCE = MetricStore.newInstance();
  }

  /**
   * Get the static metric store instance of the simulation scheduler.
   *
   * @return the metric store of the simulation.
   */
  public static MetricStore getSimulationMetricStore() {
    return MetricStoreInstanceHolder.INSTANCE;
  }

  @VisibleForTesting
  public PlanStateManager getPlanStateManager() {
    return planStateManager;
  }

  /**
   * The entrance point of the simulator. Simulate a plan by submitting a plan through this method.
   * @param submittedPhysicalPlan the plan to simulate.
   * @param maxScheduleAttempt the max number of times this plan/sub-part of the plan should be attempted.
   */
  @Override
  public void schedulePlan(final PhysicalPlan submittedPhysicalPlan, final int maxScheduleAttempt) {
    // Execute the given plan.
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

    try {
      planStateManager.waitUntilFinish();
    } finally {
      planStateManager.storeJSON("final");
    }

    LOG.info("Simulation of {} is complete!", submittedPhysicalPlan.getPlanId());
    final Long jobDuration = this.simulatedTaskExecutorMap.values().stream().mapToLong(e ->
      System.currentTimeMillis() - e.getExecutorInitializationTime()).max().orElse(0);
    this.metricStore.getOrCreateMetric(JobMetric.class, submittedPhysicalPlan.getPlanId()).setJobDuration(jobDuration);
    executorRegistry.viewExecutors(executors -> executors.forEach(executor -> metricCountDownLatch.countDown()));
  }

  /**
   * The main entry point for task scheduling.
   * This operation can be invoked at any point during job execution, as it is designed to be free of side-effects.
   * <p>
   * These are the reasons why.
   * - We 'reset' {@link PendingTaskCollectionPointer}, and not 'add' new tasks to it
   * - We make {@link TaskDispatcher} dispatch only the tasks that are READY.
   */
  private void doSchedule() {
    final java.util.Optional<List<Stage>> earliest =
      SchedulerUtils.selectEarliestSchedulableGroup(sortedScheduleGroups, planStateManager);

    if (earliest.isPresent()) {
      final List<Task> tasksToSchedule = earliest.get().stream()
        .flatMap(stage -> SchedulerUtils.selectSchedulableTasks(planStateManager, blockManagerMaster, stage).stream())
        .collect(Collectors.toList());
      if (!tasksToSchedule.isEmpty()) {
        LOG.info("Scheduling some tasks in {}, which are in the same ScheduleGroup", tasksToSchedule.stream()
          .map(Task::getTaskId)
          .map(RuntimeIdManager::getStageIdFromTaskId)
          .collect(Collectors.toSet()));

        // Set the pointer to the schedulable tasks.
        pendingTaskCollectionPointer.setToOverwrite(tasksToSchedule);

        // Notify the dispatcher that a new collection is available.
        taskDispatcher.onNewPendingTaskCollectionAvailable();
      }
    } else {
      LOG.info("Skipping this round as no ScheduleGroup is schedulable.");
    }
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

  @Override
  public void onExecutorAdded(final ExecutorRepresenter executorRepresenter) {
    LOG.info("{} added (node: {})", executorRepresenter.getExecutorId(), executorRepresenter.getNodeName());
    executorRegistry.registerExecutor(executorRepresenter);
    this.simulatedTaskExecutorMap.put(executorRepresenter.getExecutorId(),
      new SimulatedTaskExecutor(this, executorRepresenter, actualMetricStore));
    taskDispatcher.onExecutorSlotAvailable();
  }

  @Override
  public void onExecutorRemoved(final String executorId) {
    // Role of FailedEvaluatorHandler + onExecutorFailed.
    metricCountDownLatch.countDown();
    LOG.info("{} removed", executorId);

    // These are tasks that were running at the time of executor removal.
    final Set<String> interruptedTasks = new HashSet<>();
    executorRegistry.updateExecutor(executorId, (executor, state) -> {
      interruptedTasks.addAll(executor.onExecutorFailed());
      return Pair.of(executor, ExecutorRegistry.ExecutorState.FAILED);
    });

    // Retry the interrupted tasks (and required parents)
    SchedulerUtils.retryTasksAndRequiredParents(planStateManager, blockManagerMaster, interruptedTasks);

    // Trigger the scheduling of SHOULD_RETRY tasks in the earliest scheduleGroup
    doSchedule();
  }

  /**
   * Process the RuntimePassMessage.
   *
   * @param taskId that generated the message.
   * @param data   of the message.
   */
  public void onRunTimePassMessage(final String taskId, final Object data) {
    SchedulerUtils.onRunTimePassMessage(planStateManager, planRewriter, taskId, data);
  }

  @Override
  public synchronized void onTaskStateReportFromExecutor(final String executorId,
                                            final String taskId,
                                            final int attemptIdx,
                                            final TaskState.State newState,
                                            @Nullable final String taskPutOnHold,
                                            final TaskState.RecoverableTaskFailureCause failureCause) {
    // Role of MasterControlMessageReceiver + handleControlMessage --> onTaskStateChanged.
    // Do change state, as this notification is for the current task attempt.
    planStateManager.onTaskStateChanged(taskId, newState);
    switch (newState) {
      case COMPLETE:
        SchedulerUtils.onTaskExecutionComplete(executorRegistry, executorId, taskId);
        break;
      case SHOULD_RETRY:
        // SHOULD_RETRY from an executor means that the task ran into a recoverable failure
        SchedulerUtils.onTaskExecutionFailedRecoverable(planStateManager, blockManagerMaster, executorRegistry,
          executorId, taskId, failureCause);
        break;
      case ON_HOLD:
        final java.util.Optional<PhysicalPlan> optionalPhysicalPlan =
          SchedulerUtils.onTaskExecutionOnHold(planStateManager, executorRegistry, planRewriter, executorId, taskId);
        optionalPhysicalPlan.ifPresent(this::updatePlan);
        break;
      case FAILED:
        throw new UnrecoverableFailureException(new Exception(String.format("The plan failed on %s in %s",
          taskId, executorId)));
      case READY:
      case EXECUTING:
        throw new SimulationException("The states READY/EXECUTING cannot occur at this point");
      default:
        throw new UnknownExecutionStateException(new Exception("This TaskState is unknown: " + newState));
    }

    // Invoke doSchedule()
    switch (newState) {
      case COMPLETE:
      case ON_HOLD:
        // If the stage has completed
        final String stageIdForTaskUponCompletion = RuntimeIdManager.getStageIdFromTaskId(taskId);
        if (planStateManager.getStageState(stageIdForTaskUponCompletion).equals(StageState.State.COMPLETE)
          && !planStateManager.isPlanDone()) {
          doSchedule();
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

  /**
   * Handle metric message, as it would have been sent from org.apache.nemo.runtime.executor.MetricManagerWorker#send.
   * @param metricType  type of metric.
   * @param metricId    id of metric.
   * @param metricField field of metric.
   * @param metricValue value of metric.
   */
  void handleMetricMessage(final String metricType, final String metricId,
                           final String metricField, final byte[] metricValue) {
    final Class<Metric> metricClass = metricStore.getMetricClassByName(metricType);
    // process metric message
    try {
      if (metricStore.getOrCreateMetric(metricClass, metricId).processMetricMessage(metricField, metricValue)) {
        metricStore.triggerBroadcast(metricClass, metricId);
      }
    } catch (final Exception e) {
      LOG.warn("Error when processing metric message for {}, {}, {}.", metricType, metricId, metricField);
    }
  }

  /**
   * The endpoint of the simulator. Collect the metric store, and terminate the simulator.
   * @return the metrics of the simulation.
   */
  public MetricStore collectMetricStoreAndTerminate() {
    try {
      // wait for metric flush
      if (!metricCountDownLatch.await(10000, TimeUnit.MILLISECONDS)) {
        LOG.warn("Terminating master before all executor terminated messages arrived.");
      }
    } catch (final InterruptedException e) {
      LOG.warn("Waiting executor terminating process interrupted: ", e);
      // clean up state...
      Thread.currentThread().interrupt();
    }

    this.terminate();
    return this.metricStore;
  }

  @Override
  public void onSpeculativeExecutionCheck() {
    // we don't simulate speculate execution yet.
    return;
  }

  @Override
  public void terminate() {
    this.taskDispatcher.terminate();
    this.executorRegistry.terminate();
  }

  /**
   * Evaluator ActiveContext for the Simulation.
   */
  private static final class SimulationEvaluatorActiveContext implements ActiveContext {
    private final Integer id;

    /**
     * Default constructor.
     * @param id Evaluator ID.
     */
    SimulationEvaluatorActiveContext(final Integer id) {
      this.id = id;
    }

    @Override
    public void close() {
      // do nothing
    }

    @Override
    public void submitTask(final Configuration taskConf) {
      // do nothing
    }

    @Override
    public void submitContext(final Configuration contextConfiguration) {
      // do nothing
    }

    @Override
    public void submitContextAndService(final Configuration contextConfiguration,
                                        final Configuration serviceConfiguration) {
      // do nothing
    }

    @Override
    public void sendMessage(final byte[] message) {
      // do nothing
    }

    @Override
    public String getEvaluatorId() {
      return getId();
    }

    @Override
    public Optional<String> getParentId() {
      return null;
    }

    @Override
    public EvaluatorDescriptor getEvaluatorDescriptor() {
      return null;
    }

    @Override
    public String getId() {
      return "Evaluator" + id;
    }
  }

  /**
   * MessageSender for Simulations.
   */
  private static final class SimulationMessageSender implements MessageSender<ControlMessage.Message> {
    private final String executorId;
    private final SimulationScheduler scheduler;

    SimulationMessageSender(final String executorId, final SimulationScheduler scheduler) {
      this.executorId = executorId;
      this.scheduler = scheduler;
    }

    @Override
    public void send(final ControlMessage.Message message) {
      switch (message.getType()) {
        // Messages sent to the master
        case TaskStateChanged:
          final ControlMessage.TaskStateChangedMsg taskStateChangedMsg = message.getTaskStateChangedMsg();

          scheduler.onTaskStateReportFromExecutor(taskStateChangedMsg.getExecutorId(),
            taskStateChangedMsg.getTaskId(),
            taskStateChangedMsg.getAttemptIdx(),
            MessageUtils.convertTaskState(taskStateChangedMsg.getState()),
            taskStateChangedMsg.getVertexPutOnHoldId(),
            MessageUtils.convertFailureCause(taskStateChangedMsg.getFailureCause()));
          break;
        case ExecutorFailed:
          // Executor failed due to user code.
          final ControlMessage.ExecutorFailedMsg executorFailedMsg = message.getExecutorFailedMsg();
          final String failedExecutorId = executorFailedMsg.getExecutorId();
          final Exception exception = SerializationUtils.deserialize(executorFailedMsg.getException().toByteArray());
          LOG.error(failedExecutorId, " failed, Stack Trace: ", exception);
          throw new SimulationException(exception);
        case RunTimePassMessage:
          scheduler.onRunTimePassMessage(
            message.getRunTimePassMessageMsg().getTaskId(),
            message.getRunTimePassMessageMsg().getEntryList());
          break;
        case MetricMessageReceived:
          final List<ControlMessage.Metric> metricList = message.getMetricMsg().getMetricList();
          metricList.forEach(metric ->
            scheduler.handleMetricMessage(
              metric.getMetricType(), metric.getMetricId(),
              metric.getMetricField(), metric.getMetricValue().toByteArray()));
          break;
        case ExecutorDataCollected:
          final String serializedData = message.getDataCollected().getData();
          // Unsure.
          scheduler.clientRPC.send(ControlMessage.DriverToClientMessage.newBuilder()
            .setType(ControlMessage.DriverToClientMessageType.DataCollected)
            .setDataCollected(ControlMessage.DataCollectMessage.newBuilder().setData(serializedData).build())
            .build());
          break;
        //  Messages sent to the executor
        case ScheduleTask:
          final ControlMessage.ScheduleTaskMsg scheduleTaskMsg = message.getScheduleTaskMsg();
          final Task task =
            SerializationUtils.deserialize(scheduleTaskMsg.getTask().toByteArray());
          scheduler.simulatedTaskExecutorMap.get(executorId).onTaskReceived(task);
          break;
        // No metric messaging in simulation.
        case MetricFlushed:
        case RequestMetricFlush:
          break;
        default:
          throw new IllegalMessageException(
            new Exception("This message should not be received by Master or the Executor :" + message.getType()));
      }
    }

    @Override
    public <U> CompletableFuture<U> request(final ControlMessage.Message message) {
      return null;
    }

    @Override
    public void close() {
      // do nothing.
    }
  }
}
