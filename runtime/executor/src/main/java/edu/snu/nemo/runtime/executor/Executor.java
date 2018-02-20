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
package edu.snu.nemo.runtime.executor;

import com.google.protobuf.ByteString;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.conf.JobConf;
import edu.snu.nemo.common.exception.IllegalMessageException;
import edu.snu.nemo.common.exception.UnknownFailureCauseException;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.comm.ControlMessage;
import edu.snu.nemo.runtime.common.message.MessageContext;
import edu.snu.nemo.runtime.common.message.MessageEnvironment;
import edu.snu.nemo.runtime.common.message.MessageListener;
import edu.snu.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import edu.snu.nemo.runtime.common.plan.RuntimeEdge;
import edu.snu.nemo.runtime.common.plan.physical.ScheduledTaskGroup;
import edu.snu.nemo.runtime.common.plan.physical.Task;
import edu.snu.nemo.runtime.executor.data.SerializerManager;
import edu.snu.nemo.runtime.executor.datatransfer.DataTransferFactory;
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
  private final SerializerManager serializerManager;

  /**
   * Factory of InputReader/OutputWriter for executing tasks groups.
   */
  private final DataTransferFactory dataTransferFactory;

  private final PersistentConnectionToMasterMap persistentConnectionToMasterMap;

  private final MetricMessageSender metricMessageSender;

  @Inject
  public Executor(@Parameter(JobConf.ExecutorId.class) final String executorId,
                  @Parameter(JobConf.ExecutorCapacity.class) final int executorCapacity,
                  final PersistentConnectionToMasterMap persistentConnectionToMasterMap,
                  final MessageEnvironment messageEnvironment,
                  final SerializerManager serializerManager,
                  final DataTransferFactory dataTransferFactory,
                  final MetricManagerWorker metricMessageSender) {
    this.executorId = executorId;
    //this.executorService = Executors.newFixedThreadPool(executorCapacity);
    this.executorService = Executors.newCachedThreadPool();
    this.persistentConnectionToMasterMap = persistentConnectionToMasterMap;
    this.serializerManager = serializerManager;
    this.dataTransferFactory = dataTransferFactory;
    this.metricMessageSender = metricMessageSender;
    messageEnvironment.setupListener(MessageEnvironment.EXECUTOR_MESSAGE_LISTENER_ID, new ExecutorMessageReceiver());
    LOG.info("Executor [{}] capacity is " + executorCapacity, executorId);
  }

  public String getExecutorId() {
    return executorId;
  }

  private synchronized void onTaskGroupReceived(final ScheduledTaskGroup scheduledTaskGroup) {
    LOG.debug("Executor [{}] received TaskGroup [{}] to execute.",
        new Object[]{executorId, scheduledTaskGroup.getTaskGroupId()});
    executorService.execute(() -> launchTaskGroup(scheduledTaskGroup));
  }

  /**
   * Launches the TaskGroup, and keeps track of the execution state with taskGroupStateManager.
   * @param scheduledTaskGroup to launch.
   */
  private void launchTaskGroup(final ScheduledTaskGroup scheduledTaskGroup) {
    try {
      final DAG<Task, RuntimeEdge<Task>> taskGroupDag =
          SerializationUtils.deserialize(scheduledTaskGroup.getSerializedTaskGroupDag());
      final TaskGroupStateManager taskGroupStateManager =
          new TaskGroupStateManager(scheduledTaskGroup, taskGroupDag, executorId,
              persistentConnectionToMasterMap, metricMessageSender);

      scheduledTaskGroup.getTaskGroupIncomingEdges()
          .forEach(e -> serializerManager.register(e.getId(), e.getCoder(), e.getExecutionProperties()));
      scheduledTaskGroup.getTaskGroupOutgoingEdges()
          .forEach(e -> serializerManager.register(e.getId(), e.getCoder(), e.getExecutionProperties()));
      // TODO #432: remove these coders when we "streamize" task execution within a TaskGroup.
      taskGroupDag.getVertices().forEach(v -> {
        taskGroupDag.getOutgoingEdgesOf(v)
            .forEach(e -> serializerManager.register(e.getId(), e.getCoder(), e.getExecutionProperties()));
      });

      new TaskGroupExecutor(
          scheduledTaskGroup, taskGroupDag, taskGroupStateManager, dataTransferFactory, metricMessageSender).execute();
    } catch (final Exception e) {
      persistentConnectionToMasterMap.getMessageSender(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID).send(
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
