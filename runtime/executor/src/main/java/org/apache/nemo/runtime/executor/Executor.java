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
package org.apache.nemo.runtime.executor;

import com.google.protobuf.ByteString;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.executionproperty.DecoderProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DecompressionProperty;
import org.apache.nemo.common.ir.edge.executionproperty.EncoderProperty;
import org.apache.nemo.common.ir.edge.executionproperty.CompressionProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.common.exception.IllegalMessageException;
import org.apache.nemo.common.exception.UnknownFailureCauseException;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageContext;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.MessageListener;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.runtime.common.plan.RuntimeEdge;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.executor.data.BroadcastManagerWorker;
import org.apache.nemo.runtime.executor.data.SerializerManager;
import org.apache.nemo.runtime.executor.datatransfer.DataTransferFactory;
import org.apache.nemo.runtime.executor.task.TaskExecutor;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
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
   * To be used for a thread pool to execute tasks.
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

  private final BroadcastManagerWorker broadcastManagerWorker;

  private final PersistentConnectionToMasterMap persistentConnectionToMasterMap;

  private final MetricMessageSender metricMessageSender;

  @Inject
  private Executor(@Parameter(JobConf.ExecutorId.class) final String executorId,
                   final PersistentConnectionToMasterMap persistentConnectionToMasterMap,
                   final MessageEnvironment messageEnvironment,
                   final SerializerManager serializerManager,
                   final DataTransferFactory dataTransferFactory,
                   final BroadcastManagerWorker broadcastManagerWorker,
                   final MetricManagerWorker metricMessageSender) {
    this.executorId = executorId;
    this.executorService = Executors.newCachedThreadPool(new BasicThreadFactory.Builder()
        .namingPattern("TaskExecutor thread-%d")
        .build());
    this.persistentConnectionToMasterMap = persistentConnectionToMasterMap;
    this.serializerManager = serializerManager;
    this.dataTransferFactory = dataTransferFactory;
    this.broadcastManagerWorker = broadcastManagerWorker;
    this.metricMessageSender = metricMessageSender;
    messageEnvironment.setupListener(MessageEnvironment.EXECUTOR_MESSAGE_LISTENER_ID, new ExecutorMessageReceiver());
  }

  public String getExecutorId() {
    return executorId;
  }

  private synchronized void onTaskReceived(final Task task) {
    LOG.debug("Executor [{}] received Task [{}] to execute.",
        new Object[]{executorId, task.getTaskId()});
    executorService.execute(() -> launchTask(task));
  }

  /**
   * Launches the Task, and keeps track of the execution state with taskStateManager.
   * @param task to launch.
   */
  private void launchTask(final Task task) {
    try {
      final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag =
          SerializationUtils.deserialize(task.getSerializedIRDag());
      final TaskStateManager taskStateManager =
          new TaskStateManager(task, executorId, persistentConnectionToMasterMap, metricMessageSender);

      task.getTaskIncomingEdges().forEach(e -> serializerManager.register(e.getId(),
          e.getPropertyValue(EncoderProperty.class).get(),
          e.getPropertyValue(DecoderProperty.class).get(),
          e.getPropertyValue(CompressionProperty.class).orElse(null),
          e.getPropertyValue(DecompressionProperty.class).orElse(null)));
      task.getTaskOutgoingEdges().forEach(e -> serializerManager.register(e.getId(),
          e.getPropertyValue(EncoderProperty.class).get(),
          e.getPropertyValue(DecoderProperty.class).get(),
          e.getPropertyValue(CompressionProperty.class).orElse(null),
          e.getPropertyValue(DecompressionProperty.class).orElse(null)));
      irDag.getVertices().forEach(v -> {
        irDag.getOutgoingEdgesOf(v).forEach(e -> serializerManager.register(e.getId(),
            e.getPropertyValue(EncoderProperty.class).get(),
            e.getPropertyValue(DecoderProperty.class).get(),
            e.getPropertyValue(CompressionProperty.class).orElse(null),
            e.getPropertyValue(DecompressionProperty.class).orElse(null)));
      });

      new TaskExecutor(task, irDag, taskStateManager, dataTransferFactory, broadcastManagerWorker,
          metricMessageSender, persistentConnectionToMasterMap).execute();
    } catch (final Exception e) {
      persistentConnectionToMasterMap.getMessageSender(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID).send(
          ControlMessage.Message.newBuilder()
              .setId(RuntimeIdManager.generateMessageId())
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
        case ScheduleTask:
          final ControlMessage.ScheduleTaskMsg scheduleTaskMsg = message.getScheduleTaskMsg();
          final Task task =
              SerializationUtils.deserialize(scheduleTaskMsg.getTask().toByteArray());
          onTaskReceived(task);
          break;
        case RequestMetricFlush:
          metricMessageSender.flush();
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
