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
package edu.snu.onyx.runtime.executor;

import com.google.protobuf.ByteString;
import edu.snu.onyx.client.JobConf;
import edu.snu.onyx.runtime.common.RuntimeIdGenerator;
import edu.snu.onyx.runtime.common.grpc.Common;
import edu.snu.onyx.runtime.common.metric.MetricMessageSender;
import edu.snu.onyx.runtime.common.plan.physical.ScheduledTaskGroup;
import edu.snu.onyx.runtime.exception.UnknownFailureCauseException;
import edu.snu.onyx.runtime.executor.data.PartitionManagerWorker;
import edu.snu.onyx.runtime.executor.datatransfer.DataTransferFactory;
import edu.snu.onyx.runtime.executor.grpc.ExecutorScheduler;
import edu.snu.onyx.runtime.executor.grpc.ExecutorSchedulerServiceGrpc;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executor.
 */
public final class Executor {
  private static final Logger LOG = LoggerFactory.getLogger(Executor.class.getName());

  private final String executorId;

  /**
   * To be used for a thread pool to execute task groups.
   */
  private final ExecutorService executorService;

  /**
   * In charge of this executor's intermediate data transfer.
   */
  private final PartitionManagerWorker partitionManagerWorker;

  /**
   * Factory of InputReader/OutputWriter for executing tasks groups.
   */
  private final DataTransferFactory dataTransferFactory;

  private final MasterRPC masterRPC;

  private final MetricMessageSender metricMessageSender;

  @Inject
  public Executor(@Parameter(JobConf.ExecutorId.class) final String executorId,
                  @Parameter(JobConf.ExecutorCapacity.class) final int executorCapacity,
                  final MasterRPC masterRPC,
                  final MessageEnvironment messageEnvironment,
                  final PartitionManagerWorker partitionManagerWorker,
                  final DataTransferFactory dataTransferFactory,
                  final MetricManagerWorker metricMessageSender) {
    this.executorId = executorId;
    this.executorService = Executors.newFixedThreadPool(executorCapacity);
    this.masterRPC = masterRPC;
    this.partitionManagerWorker = partitionManagerWorker;
    this.dataTransferFactory = dataTransferFactory;
    this.metricMessageSender = metricMessageSender;
  }

  public String getExecutorId() {
    return executorId;
  }

  private synchronized void onTaskGroupReceived(final ScheduledTaskGroup scheduledTaskGroup) {
    LOG.debug("Executor [{}] received TaskGroup [{}] to execute.",
        new Object[]{executorId, scheduledTaskGroup.getTaskGroup().getTaskGroupId()});
    executorService.execute(() -> launchTaskGroup(scheduledTaskGroup));
  }

  /**
   * Launches the TaskGroup, and keeps track of the execution state with taskGroupStateManager.
   * @param scheduledTaskGroup to launch.
   */
  private void launchTaskGroup(final ScheduledTaskGroup scheduledTaskGroup) {
    try {
      final TaskGroupStateManager taskGroupStateManager =
          new TaskGroupStateManager(scheduledTaskGroup.getTaskGroup(), scheduledTaskGroup.getAttemptIdx(), executorId,
              masterRPC,
              metricMessageSender);

      scheduledTaskGroup.getTaskGroupIncomingEdges()
          .forEach(e -> partitionManagerWorker.registerCoder(e.getId(), e.getCoder()));
      scheduledTaskGroup.getTaskGroupOutgoingEdges()
          .forEach(e -> partitionManagerWorker.registerCoder(e.getId(), e.getCoder()));

      new TaskGroupExecutor(scheduledTaskGroup.getTaskGroup(),
          taskGroupStateManager,
          scheduledTaskGroup.getTaskGroupIncomingEdges(),
          scheduledTaskGroup.getTaskGroupOutgoingEdges(),
          dataTransferFactory,
          partitionManagerWorker).execute();
    } catch (final Exception e) {
      masterRPC.getMessageSender(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID).send(
          ControlMessage.Message.newBuilder()
              .setId(RuntimeIdGenerator.generateMessageId())
              .setListenerId(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID)
              .setType(ControlMessage.MessageType.ExecutorFailed)
              .setExecutorFailedMsg(ControlMessage.ExecutorFailedMsg.newBuilder()
                  .setExecutorId(executorId)
                  .setException(ByteString.copyFrom(SerializationUtils.serialize(e)))
                  .build())
              .build());
      throw e;
    } finally {
      terminate();
    }
  }

  public void terminate() {
    try {
      metricMessageSender.close();
    } catch (final UnknownFailureCauseException e) {
      throw new UnknownFailureCauseException(
          new Exception("Closing MetricManagerWorker failed in executor " + executorId));
    }
  }

  private class ExecutorSchedulerService extends ExecutorSchedulerServiceGrpc.ExecutorSchedulerServiceImplBase {
    private final Common.Empty empty = Common.Empty.newBuilder().build();

    @Override
    public void executeTaskGroup(final ExecutorScheduler.TaskGroupExecutionRequest request,
                                 final StreamObserver<Common.Empty> observer) {
      final ScheduledTaskGroup scheduledTaskGroup =
          SerializationUtils.deserialize(request.getTaskGroup().toByteArray());
      onTaskGroupReceived(scheduledTaskGroup);
      observer.onNext(empty);
      observer.onCompleted();
    }
  }
}
