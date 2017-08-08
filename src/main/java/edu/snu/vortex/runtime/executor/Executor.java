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
package edu.snu.vortex.runtime.executor;

import com.google.protobuf.ByteString;
import edu.snu.vortex.client.JobConf;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.common.message.MessageContext;
import edu.snu.vortex.runtime.common.message.MessageEnvironment;
import edu.snu.vortex.runtime.common.message.MessageListener;
import edu.snu.vortex.runtime.common.plan.physical.ScheduledTaskGroup;
import edu.snu.vortex.runtime.exception.IllegalMessageException;
import edu.snu.vortex.runtime.executor.data.PartitionManagerWorker;
import edu.snu.vortex.runtime.executor.datatransfer.DataTransferFactory;
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

  private TaskGroupStateManager taskGroupStateManager;

  private final PersistentConnectionToMaster persistentConnectionToMaster;

  @Inject
  public Executor(@Parameter(JobConf.ExecutorId.class) final String executorId,
                  @Parameter(JobConf.ExecutorCapacity.class) final int executorCapacity,
                  final PersistentConnectionToMaster persistentConnectionToMaster,
                  final MessageEnvironment messageEnvironment,
                  final PartitionManagerWorker partitionManagerWorker,
                  final DataTransferFactory dataTransferFactory) {
    this.executorId = executorId;
    this.executorService = Executors.newFixedThreadPool(executorCapacity);
    this.partitionManagerWorker = partitionManagerWorker;
    this.dataTransferFactory = dataTransferFactory;
    this.persistentConnectionToMaster = persistentConnectionToMaster;
    messageEnvironment.setupListener(MessageEnvironment.EXECUTOR_MESSAGE_RECEIVER, new ExecutorMessageReceiver());

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
      taskGroupStateManager =
          new TaskGroupStateManager(scheduledTaskGroup.getTaskGroup(), scheduledTaskGroup.getAttemptIdx(), executorId,
              persistentConnectionToMaster);

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
      persistentConnectionToMaster.getMessageSender().send(
          ControlMessage.Message.newBuilder()
              .setId(RuntimeIdGenerator.generateMessageId())
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

  }

  /**
   * MessageListener for Executor.
   */
  private final class ExecutorMessageReceiver implements MessageListener<ControlMessage.Message> {

    @Override
    public void onMessage(final ControlMessage.Message message) {
      switch (message.getType()) {
      case ScheduleTaskGroup:
        final ControlMessage.ScheduleTaskGroupMsg scheduleTaskGroupMsg = message.getScheduleTaskGroupMsg();
        final ScheduledTaskGroup scheduledTaskGroup =
            SerializationUtils.deserialize(scheduleTaskGroupMsg.getTaskGroup().toByteArray());
        onTaskGroupReceived(scheduledTaskGroup);
        break;
      default:
        throw new IllegalMessageException(
            new Exception("This message should not be received by an executor :" + message.getType()));
      }
    }

    @Override
    public void onMessageWithContext(final ControlMessage.Message message, final MessageContext messageContext) {
      switch (message.getType()) {
      default:
        throw new IllegalMessageException(
            new Exception("This message should not be requested to an executor :" + message.getType()));
      }
    }
  }
}
