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

import edu.snu.vortex.client.JobConf;
import edu.snu.vortex.common.proxy.ClientEndpoint;
import edu.snu.vortex.common.proxy.DriverEndpoint;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.common.message.MessageContext;
import edu.snu.vortex.runtime.common.message.MessageEnvironment;
import edu.snu.vortex.runtime.common.message.MessageListener;
import edu.snu.vortex.runtime.common.plan.logical.ExecutionPlan;
import edu.snu.vortex.runtime.common.plan.logical.Stage;
import edu.snu.vortex.runtime.common.plan.logical.StageEdge;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalDAGGenerator;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalPlan;
import edu.snu.vortex.runtime.common.state.PartitionState;
import edu.snu.vortex.runtime.common.state.TaskGroupState;
import edu.snu.vortex.runtime.exception.IllegalMessageException;
import edu.snu.vortex.runtime.exception.UnknownExecutionStateException;
import edu.snu.vortex.runtime.exception.UnknownFailureCauseException;
import edu.snu.vortex.runtime.executor.partition.PartitionManagerWorker;
import edu.snu.vortex.runtime.master.resource.ContainerManager;
import edu.snu.vortex.runtime.master.scheduler.Scheduler;
import org.apache.beam.sdk.repackaged.org.apache.commons.lang3.SerializationUtils;
import edu.snu.vortex.common.dag.DAG;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Runtime Master is the central controller of Runtime.
 * Compiler submits an {@link ExecutionPlan} to Runtime Master to execute a job.
 * Runtime Master handles:
 *    a) Physical conversion of a job's DAG into a physical plan.
 *    b) Scheduling the job with {@link Scheduler}.
 *    c) (Please list others done by Runtime Master as features are added).
 */
public final class RuntimeMaster {
  private static final Logger LOG = Logger.getLogger(RuntimeMaster.class.getName());
  private static final int DAG_LOGGING_PERIOD = 3000;

  private final Scheduler scheduler;
  private final ContainerManager containerManager;
  private final MessageEnvironment masterMessageEnvironment;
  private final PartitionManagerMaster partitionManagerMaster;
  private JobStateManager jobStateManager;

  private final String dagDirectory;
  private PhysicalPlan physicalPlan;
  private final int maxScheduleAttempt;

  @Inject
  public RuntimeMaster(final Scheduler scheduler,
                       final ContainerManager containerManager,
                       final MessageEnvironment masterMessageEnvironment,
                       final PartitionManagerMaster partitionManagerMaster,
                       @Parameter(JobConf.DAGDirectory.class) final String dagDirectory,
                       @Parameter(JobConf.MaxScheduleAttempt.class) final int maxScheduleAttempt) {
    this.scheduler = scheduler;
    this.maxScheduleAttempt = maxScheduleAttempt;
    this.containerManager = containerManager;
    this.masterMessageEnvironment = masterMessageEnvironment;
    this.masterMessageEnvironment
        .setupListener(MessageEnvironment.MASTER_MESSAGE_RECEIVER, new MasterMessageReceiver());
    this.partitionManagerMaster = partitionManagerMaster;
    this.dagDirectory = dagDirectory;
  }

  /**
   * Submits the {@link ExecutionPlan} to Runtime.
   * @param executionPlan to execute.
   * @param clientEndpoint of this plan.
   */
  public void execute(final ExecutionPlan executionPlan,
                      final ClientEndpoint clientEndpoint) {
    physicalPlan = generatePhysicalPlan(executionPlan);
    try {
      jobStateManager = scheduler.scheduleJob(physicalPlan, maxScheduleAttempt);
      final DriverEndpoint driverEndpoint = new DriverEndpoint(jobStateManager, clientEndpoint);

      // Schedule dag logging thread
      final ScheduledExecutorService dagLoggingExecutor = scheduleDagLogging();

      // Wait the job to finish and stop logging
      jobStateManager.waitUntilFinish();
      dagLoggingExecutor.shutdown();

      jobStateManager.storeJSON(dagDirectory, "final");
      LOG.log(Level.INFO, "{0} is complete!", executionPlan.getId());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void terminate() {
    containerManager.terminate();
  }

  /**
   * Generates the {@link PhysicalPlan} to be executed.
   * @param executionPlan that should be converted to a physical plan
   * @return {@link PhysicalPlan} to execute.
   */
  private PhysicalPlan generatePhysicalPlan(final ExecutionPlan executionPlan) {
    final DAG<Stage, StageEdge> logicalDAG = executionPlan.getRuntimeStageDAG();
    logicalDAG.storeJSON(dagDirectory, "plan-logical", "logical execution plan");

    final PhysicalPlan plan = new PhysicalPlan(executionPlan.getId(),
        logicalDAG.convert(new PhysicalDAGGenerator()));
    plan.getStageDAG().storeJSON(dagDirectory, "plan-physical", "physical execution plan");
    return plan;
  }

  /**
   * Handler for messages received by Master.
   */
  public final class MasterMessageReceiver implements MessageListener<ControlMessage.Message> {

    @Override
    public void onMessage(final ControlMessage.Message message) {
      switch (message.getType()) {
      case TaskGroupStateChanged:
        final ControlMessage.TaskGroupStateChangedMsg taskGroupStateChangedMsg = message.getTaskStateChangedMsg();
        scheduler.onTaskGroupStateChanged(taskGroupStateChangedMsg.getExecutorId(),
            taskGroupStateChangedMsg.getTaskGroupId(),
            convertTaskGroupState(taskGroupStateChangedMsg.getState()),
            taskGroupStateChangedMsg.getAttemptIdx(),
            taskGroupStateChangedMsg.getFailedTaskIdsList(),
            convertFailureCause(taskGroupStateChangedMsg.getFailureCause()));
        break;
      case PartitionStateChanged:
        final ControlMessage.PartitionStateChangedMsg partitionStateChangedMsg = message.getPartitionStateChangedMsg();
        partitionManagerMaster.onPartitionStateChanged(
            partitionStateChangedMsg.getExecutorId(), partitionStateChangedMsg.getPartitionId(),
            convertPartitionState(partitionStateChangedMsg.getState()));
        break;
      case ExecutorFailed:
        final ControlMessage.ExecutorFailedMsg executorFailedMsg = message.getExecutorFailedMsg();
        final String failedExecutorId = executorFailedMsg.getExecutorId();
        final Exception exception = SerializationUtils.deserialize(executorFailedMsg.getException().toByteArray());
        LOG.log(Level.SEVERE, failedExecutorId + " failed, Stack Trace: ", exception);
        containerManager.onExecutorRemoved(failedExecutorId);
        throw new RuntimeException(exception);
      default:
        throw new IllegalMessageException(
            new Exception("This message should not be received by Master :" + message.getType()));
      }
    }

    @Override
    public void onMessageWithContext(final ControlMessage.Message message, final MessageContext messageContext) {
      switch (message.getType()) {
      case RequestPartitionLocation:
        final ControlMessage.RequestPartitionLocationMsg requestPartitionLocationMsg =
            message.getRequestPartitionLocationMsg();
        final Optional<String> executorId =
            partitionManagerMaster.getPartitionLocation(requestPartitionLocationMsg.getPartitionId());
        messageContext.reply(
            ControlMessage.Message.newBuilder()
                .setId(RuntimeIdGenerator.generateMessageId())
                .setType(ControlMessage.MessageType.PartitionLocationInfo)
                .setPartitionLocationInfoMsg(
                    ControlMessage.PartitionLocationInfoMsg.newBuilder()
                        .setRequestId(message.getId())
                        .setPartitionId(requestPartitionLocationMsg.getPartitionId())
                        .setOwnerExecutorId(executorId.isPresent()
                            ? executorId.get()
                            : PartitionManagerWorker.NO_REMOTE_PARTITION)
                        .build())
                .build());
        break;
      default:
        throw new IllegalMessageException(
            new Exception("This message should not be requested to Master :" + message.getType()));
      }
    }
  }

  // TODO #164: Cleanup Protobuf Usage
  private TaskGroupState.State convertTaskGroupState(final ControlMessage.TaskGroupStateFromExecutor state) {
    switch (state) {
    case READY:
      return TaskGroupState.State.READY;
    case EXECUTING:
      return TaskGroupState.State.EXECUTING;
    case COMPLETE:
      return TaskGroupState.State.COMPLETE;
    case FAILED_RECOVERABLE:
      return TaskGroupState.State.FAILED_RECOVERABLE;
    case FAILED_UNRECOVERABLE:
      return TaskGroupState.State.FAILED_UNRECOVERABLE;
    default:
      throw new UnknownExecutionStateException(new Exception("This TaskGroupState is unknown: " + state));
    }
  }

  // TODO #164: Cleanup Protobuf Usage
  private PartitionState.State convertPartitionState(final ControlMessage.PartitionStateFromExecutor state) {
    switch (state) {
    case PARTITION_READY:
      return PartitionState.State.READY;
    case MOVING:
      return PartitionState.State.MOVING;
    case COMMITTED:
      return PartitionState.State.COMMITTED;
    case LOST:
      return PartitionState.State.LOST;
    case REMOVED:
      return PartitionState.State.REMOVED;
    default:
      throw new UnknownExecutionStateException(new Exception("This PartitionState is unknown: " + state));
    }
  }

  // TODO #164: Cleanup Protobuf Usage
  private TaskGroupState.RecoverableFailureCause convertFailureCause(
      final ControlMessage.RecoverableFailureCause cause) {
    switch (cause) {
    case InputReadFailure:
      return TaskGroupState.RecoverableFailureCause.INPUT_READ_FAILURE;
    case OutputWriteFailure:
      return TaskGroupState.RecoverableFailureCause.OUTPUT_WRITE_FAILURE;
    default:
      throw new UnknownFailureCauseException(new Throwable("The failure cause for the recoverable failure is unknown"));
    }
  }

  /**
   * Schedules a periodic DAG logging thread.
   * TODO #58: Web UI (Real-time visualization)
   *
   * @return the scheduled executor service.
   */
  private ScheduledExecutorService scheduleDagLogging() {
    final ScheduledExecutorService dagLoggingExecutor = Executors.newSingleThreadScheduledExecutor();
    dagLoggingExecutor.scheduleAtFixedRate(new Runnable() {
      private int dagLogFileIndex = 0;

      public void run() {
        jobStateManager.storeJSON(dagDirectory, String.valueOf(dagLogFileIndex++));
      }
    }, DAG_LOGGING_PERIOD, DAG_LOGGING_PERIOD, TimeUnit.MILLISECONDS);

    return dagLoggingExecutor;
  }
}
