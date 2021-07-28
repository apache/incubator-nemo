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
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.coder.BytesDecoderFactory;
import org.apache.nemo.common.coder.BytesEncoderFactory;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.exception.IllegalMessageException;
import org.apache.nemo.common.exception.UnknownFailureCauseException;
import org.apache.nemo.common.ir.edge.executionproperty.CompressionProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DecoderProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DecompressionProperty;
import org.apache.nemo.common.ir.edge.executionproperty.EncoderProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.conf.JobConf;
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
import org.apache.nemo.runtime.executor.datatransfer.IntermediateDataIOFactory;
import org.apache.nemo.runtime.executor.datatransfer.NemoEventDecoderFactory;
import org.apache.nemo.runtime.executor.datatransfer.NemoEventEncoderFactory;
import org.apache.nemo.runtime.executor.task.TaskExecutor;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
  /* For work stealing */
  private final ScheduledExecutorService workStealingManager;

  /**
   * In charge of this executor's intermediate data transfer.
   */
  private final SerializerManager serializerManager;

  /**
   * Factory of InputReader/OutputWriter for executing tasks groups.
   */
  private final IntermediateDataIOFactory intermediateDataIOFactory;

  private final BroadcastManagerWorker broadcastManagerWorker;

  private final PersistentConnectionToMasterMap persistentConnectionToMasterMap;

  private final MetricMessageSender metricMessageSender;

  /**
   * For runtime optimizations.
   */
  private final List<Pair<TaskExecutor, AtomicBoolean>> listOfWorkingTaskExecutors;
  private final Map<String, Pair<AtomicInteger, AtomicInteger>> taskIdToIteratorInfo;

  @Inject
  private Executor(@Parameter(JobConf.ExecutorId.class) final String executorId,
                   final PersistentConnectionToMasterMap persistentConnectionToMasterMap,
                   final MessageEnvironment messageEnvironment,
                   final SerializerManager serializerManager,
                   final IntermediateDataIOFactory intermediateDataIOFactory,
                   final BroadcastManagerWorker broadcastManagerWorker,
                   final MetricManagerWorker metricMessageSender) {
    this.executorId = executorId;
    this.executorService = Executors.newCachedThreadPool(new BasicThreadFactory.Builder()
      .namingPattern("TaskExecutor thread-%d")
      .build());
    this.workStealingManager = Executors.newSingleThreadScheduledExecutor(new BasicThreadFactory.Builder()
      .namingPattern("workstealing manager in executorSide")
      .build());
    this.persistentConnectionToMasterMap = persistentConnectionToMasterMap;
    this.serializerManager = serializerManager;
    this.intermediateDataIOFactory = intermediateDataIOFactory;
    this.broadcastManagerWorker = broadcastManagerWorker;
    this.metricMessageSender = metricMessageSender;
    messageEnvironment.setupListener(MessageEnvironment.EXECUTOR_MESSAGE_LISTENER_ID, new ExecutorMessageReceiver());

    this.listOfWorkingTaskExecutors = Collections.synchronizedList(new LinkedList<>());
    this.taskIdToIteratorInfo = new ConcurrentHashMap();

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
   *
   * @param task to launch.
   */
  private void launchTask(final Task task) {
    LOG.info("Launch task: {}", task.getTaskId());
    try {
      final long deserializationStartTime = System.currentTimeMillis();
      final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag =
        SerializationUtils.deserialize(task.getSerializedIRDag());
      metricMessageSender.send("TaskMetric", task.getTaskId(), "taskDeserializationTime",
        SerializationUtils.serialize(System.currentTimeMillis() - deserializationStartTime));
      final TaskStateManager taskStateManager =
        new TaskStateManager(task, executorId, persistentConnectionToMasterMap, metricMessageSender);

      task.getTaskIncomingEdges().forEach(e -> serializerManager.register(e.getId(),
        getEncoderFactory(e.getPropertyValue(EncoderProperty.class).get()),
        getDecoderFactory(e.getPropertyValue(DecoderProperty.class).get()),
        e.getPropertyValue(CompressionProperty.class).orElse(null),
        e.getPropertyValue(DecompressionProperty.class).orElse(null)));
      task.getTaskOutgoingEdges().forEach(e -> serializerManager.register(e.getId(),
        getEncoderFactory(e.getPropertyValue(EncoderProperty.class).get()),
        getDecoderFactory(e.getPropertyValue(DecoderProperty.class).get()),
        e.getPropertyValue(CompressionProperty.class).orElse(null),
        e.getPropertyValue(DecompressionProperty.class).orElse(null)));
      irDag.getVertices().forEach(v ->
        irDag.getOutgoingEdgesOf(v).forEach(e -> serializerManager.register(e.getId(),
          getEncoderFactory(e.getPropertyValue(EncoderProperty.class).get()),
          getDecoderFactory(e.getPropertyValue(DecoderProperty.class).get()),
          e.getPropertyValue(CompressionProperty.class).orElse(null),
          e.getPropertyValue(DecompressionProperty.class).orElse(null))));

      final AtomicBoolean onHold = new AtomicBoolean(false);
      final TaskExecutor taskExecutor = new TaskExecutor(task, irDag, taskStateManager, intermediateDataIOFactory,
        broadcastManagerWorker, metricMessageSender, persistentConnectionToMasterMap, onHold);
      Pair<TaskExecutor, AtomicBoolean> taskExecutorPair = Pair.of(taskExecutor, onHold);

      listOfWorkingTaskExecutors.add(taskExecutorPair);
      taskIdToIteratorInfo.put(task.getTaskId(),
        Pair.of(task.getIteratorStartIndex(), task.getIteratorEndIndex()));
      taskExecutor.execute();
      listOfWorkingTaskExecutors.remove(taskExecutorPair);
      taskIdToIteratorInfo.remove(task.getTaskId());

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

  /**
   * This wraps the encoder with NemoEventEncoder.
   * If the encoder is BytesEncoderFactory, we do not wrap the encoder.
   * TODO #276: Add NoCoder property value in Encoder/DecoderProperty
   *
   * @param encoderFactory encoder factory
   * @return wrapped encoder
   */
  private EncoderFactory getEncoderFactory(final EncoderFactory encoderFactory) {
    if (encoderFactory instanceof BytesEncoderFactory) {
      return encoderFactory;
    } else {
      return new NemoEventEncoderFactory(encoderFactory);
    }
  }

  /**
   * This wraps the encoder with NemoEventDecoder.
   * If the decoder is BytesDecoderFactory, we do not wrap the decoder.
   * TODO #276: Add NoCoder property value in Encoder/DecoderProperty
   *
   * @param decoderFactory decoder factory
   * @return wrapped decoder
   */
  private DecoderFactory getDecoderFactory(final DecoderFactory decoderFactory) {
    if (decoderFactory instanceof BytesDecoderFactory) {
      return decoderFactory;
    } else {
      return new NemoEventDecoderFactory(decoderFactory);
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

  /* Methods for work stealing */
  private synchronized void onDataRequestReceived() {
    listOfWorkingTaskExecutors.forEach(pair -> pair.right().set(true));
    //listOfWorkingTaskExecutors.forEach(TaskExecutor::onRequestForProcessedData);
  }

  private synchronized void resumePausedTasks() {
    listOfWorkingTaskExecutors.forEach(pair -> pair.right().set(false));
  }

  private synchronized void resumePausedTasksWithWorkStealing(final Map<String, Pair<Integer, Integer>> result) {
    // update iterator information
    // skewed tasks: set iterator value
    // non skewed tasks: do not change iterator
    for (String taskId : taskIdToIteratorInfo.keySet()) {
      Pair<Integer, Integer> startAndEndIndex = result.get(taskId);
      if (startAndEndIndex.left() == 0 && startAndEndIndex.right() == Integer.MAX_VALUE) {
        continue;
      }
      Pair<AtomicInteger, AtomicInteger> currentInfo = taskIdToIteratorInfo.get(taskId);
      currentInfo.left().set(startAndEndIndex.left()); // null pointer exception here!
      currentInfo.right().set(startAndEndIndex.right());
    }
    resumePausedTasks();
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
        case HaltExecutors:
          onDataRequestReceived();
          break;
        case ResumeTask:
          resumePausedTasks();
          break;
        case SendWorkStealingResult:
          final ControlMessage.WorkStealingResultMsg workStealingResultMsg = message.getSendWorkStealingResult();
          final Map<String, Pair<Integer, Integer>> iteratorInformationMap =
            SerializationUtils.deserialize(workStealingResultMsg.getWorkStealingResult().toByteArray());
          LOG.error("received: {}", iteratorInformationMap);
          resumePausedTasksWithWorkStealing(iteratorInformationMap);
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
