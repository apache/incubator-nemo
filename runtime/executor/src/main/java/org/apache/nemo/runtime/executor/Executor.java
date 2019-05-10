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
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.offloading.common.OffloadingWorkerFactory;
import org.apache.nemo.offloading.common.ServerlessExecutorProvider;
import org.apache.nemo.common.coder.BytesDecoderFactory;
import org.apache.nemo.common.coder.BytesEncoderFactory;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.coder.EncoderFactory;
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
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.common.state.TaskState;
import org.apache.nemo.runtime.executor.bytetransfer.ByteTransport;
import org.apache.nemo.runtime.executor.common.ExecutorThread;
import org.apache.nemo.runtime.executor.data.PipeManagerWorker;
import org.apache.nemo.runtime.executor.data.SerializerManager;
import org.apache.nemo.runtime.executor.datatransfer.IntermediateDataIOFactory;
import org.apache.nemo.runtime.executor.datatransfer.TaskInputContextMap;
import org.apache.nemo.runtime.executor.common.TaskExecutor;
import org.apache.nemo.runtime.executor.data.BroadcastManagerWorker;
import org.apache.nemo.runtime.executor.common.NemoEventDecoderFactory;
import org.apache.nemo.runtime.executor.common.NemoEventEncoderFactory;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.nemo.runtime.executor.task.DefaultTaskExecutorImpl;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executor.
 */
public final class Executor {
  private static final Logger LOG = LoggerFactory.getLogger(Executor.class.getName());

  private final String executorId;

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

  private final ServerlessExecutorProvider serverlessExecutorProvider;

  private volatile boolean started = false;

  private final ConcurrentMap<TaskExecutor, Boolean> taskExecutorMap;
  private final ExecutorService executorService;

  private final EvalConf evalConf;
  private final OffloadingWorkerFactory offloadingWorkerFactory;

  private final TaskOffloader taskOffloader;

  private final ByteTransport byteTransport;
  private final PipeManagerWorker pipeManagerWorker;

  private ScheduledExecutorService scheduledExecutorService;
  private final ExecutorService taskEventExecutorService;

  private final List<ExecutorThread> executorThreads;

  private final AtomicInteger numReceivedTasks = new AtomicInteger(0);
  private final TaskInputContextMap taskInputContextMap;

  @Inject
  private Executor(@Parameter(JobConf.ExecutorId.class) final String executorId,
                   final PersistentConnectionToMasterMap persistentConnectionToMasterMap,
                   final MessageEnvironment messageEnvironment,
                   final SerializerManager serializerManager,
                   final IntermediateDataIOFactory intermediateDataIOFactory,
                   final BroadcastManagerWorker broadcastManagerWorker,
                   final MetricManagerWorker metricMessageSender,
                   final ServerlessExecutorProvider serverlessExecutorProvider,
                   final TaskOffloader taskOffloader,
                   final ByteTransport byteTransport,
                   //final CpuBottleneckDetector bottleneckDetector,
                   final OffloadingWorkerFactory offloadingWorkerFactory,
                   final EvalConf evalConf,
                   final SystemLoadProfiler profiler,
                   final PipeManagerWorker pipeManagerWorker,
                   final TaskExecutorMapWrapper taskExecutorMapWrapper,
                   final TaskInputContextMap taskInputContextMap) {
                   //@Parameter(EvalConf.BottleneckDetectionCpuThreshold.class) final double threshold,
                   //final CpuEventModel cpuEventModel) {
    this.executorId = executorId;
    this.byteTransport = byteTransport;
    this.pipeManagerWorker = pipeManagerWorker;
    this.taskEventExecutorService = Executors.newSingleThreadExecutor();
    this.taskInputContextMap = taskInputContextMap;
    this.executorService = Executors.newCachedThreadPool(new BasicThreadFactory.Builder()
              .namingPattern("TaskExecutor thread-%d")
              .build());
    this.persistentConnectionToMasterMap = persistentConnectionToMasterMap;
    this.serializerManager = serializerManager;
    this.intermediateDataIOFactory = intermediateDataIOFactory;
    this.broadcastManagerWorker = broadcastManagerWorker;
    this.taskOffloader = taskOffloader;
    this.metricMessageSender = metricMessageSender;
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    scheduledExecutorService.scheduleAtFixedRate(() -> {
      LOG.info("Cpu load: {}", profiler.getCpuLoad());
    }, 1, 1, TimeUnit.SECONDS);

    this.evalConf = evalConf;
    LOG.info("\n{}", evalConf);
    this.serverlessExecutorProvider = serverlessExecutorProvider;
    this.offloadingWorkerFactory = offloadingWorkerFactory;
    this.taskExecutorMap = taskExecutorMapWrapper.taskExecutorMap;
    messageEnvironment.setupListener(MessageEnvironment.EXECUTOR_MESSAGE_LISTENER_ID, new ExecutorMessageReceiver());

    this.executorThreads = new ArrayList<>(evalConf.executorThreadNum);
    for (int i = 0; i < evalConf.executorThreadNum; i++) {
      executorThreads.add(new ExecutorThread(scheduledExecutorService, i, executorId));
      executorThreads.get(i).start();
    }
  }

  public String getExecutorId() {
    return executorId;
  }

  private synchronized void onTaskReceived(final Task task) {
    LOG.info("Executor [{}] received Task [{}] to execute.",
        new Object[]{executorId, task.getTaskId()});

    final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag =
      SerializationUtils.deserialize(task.getSerializedIRDag());

    if (!started) {

      if (evalConf.enableOffloading) {
        taskOffloader.start();
      }

      if (evalConf.offloadingdebug) {
        taskOffloader.startDownstreamDebugging();
        //taskOffloader.startDebugging();
      }

      //bottleneckDetector.setBottleneckHandler(new BottleneckHandler());
      //bottleneckDetector.start();
      started = true;
    }

    executorService.execute(() -> launchTask(task, irDag));
  }


  /**
   * Write the object to a Base64 string.
   * @param obj object
   * @return serialized object
   * @throws IOException
   */
  public static String serializeToString(final Serializable obj) {
    try {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(obj);
      oos.close();
      return Base64.getEncoder().encodeToString(baos.toByteArray());
    } catch (final IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  /**
   * Launches the Task, and keeps track of the execution state with taskStateManager.
   * @param task to launch.
   */
  private void launchTask(final Task task,
                          final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag) {
    LOG.info("Launch task: {}", task.getTaskId());

    try {
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
      irDag.getVertices().forEach(v -> {
        irDag.getOutgoingEdgesOf(v).forEach(e -> serializerManager.register(e.getId(),
            getEncoderFactory(e.getPropertyValue(EncoderProperty.class).get()),
            getDecoderFactory(e.getPropertyValue(DecoderProperty.class).get()),
            e.getPropertyValue(CompressionProperty.class).orElse(null),
            e.getPropertyValue(DecompressionProperty.class).orElse(null)));
      });


      final TaskExecutor taskExecutor =
      new DefaultTaskExecutorImpl(
        Thread.currentThread().getId(),
        executorId,
        byteTransport,
        persistentConnectionToMasterMap,
        pipeManagerWorker,
        task,
        irDag,
        taskStateManager,
        intermediateDataIOFactory,
        broadcastManagerWorker,
        metricMessageSender,
        persistentConnectionToMasterMap,
        serializerManager,
        serverlessExecutorProvider,
        offloadingWorkerFactory,
        evalConf,
        taskInputContextMap);

      taskExecutorMap.put(taskExecutor, true);
      final int numTask = numReceivedTasks.getAndIncrement();
      final int index = numTask % evalConf.executorThreadNum;

      LOG.info("Add Task {} to {} thread of {}", taskExecutor.getId(), index, executorId);

      executorThreads.get(index).addNewTask(taskExecutor);
      //taskExecutor.execute();
      taskStateManager.onTaskStateChanged(TaskState.State.EXECUTING, Optional.empty(), Optional.empty());

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
   * This wraps the encoder with OffloadingEventEncoder.
   * If the encoder is BytesEncoderFactory, we do not wrap the encoder.
   * TODO #276: Add NoCoder property value in Encoder/DecoderProperty
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
   * This wraps the encoder with OffloadingEventDecoder.
   * If the decoder is BytesDecoderFactory, we do not wrap the decoder.
   * TODO #276: Add NoCoder property value in Encoder/DecoderProperty
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
      executorThreads.forEach(ExecutorThread::close);
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

  /*
  final class BottleneckHandler implements EventHandler<CpuBottleneckDetector.BottleneckEvent> {

    private List<TaskExecutor> prevOffloadingExecutors = new ArrayList<>();
    private long prevEndTime = System.currentTimeMillis();
    private long slackTime = 10000;
    private boolean started = false;

    @Override
    public void onNext(final CpuBottleneckDetector.BottleneckEvent event) {
      LOG.info("Bottleneck event: {}", event);
      if (evalConf.enableOffloading) {
        switch (event.type) {
          case START: {

            if (!(slackTime >= System.currentTimeMillis() - prevEndTime)) {
              // skip
              LOG.info("Skip start event!");
            } else {
              started = true;

              // estimate desirable events
              final int desirableEvents = cpuEventModel.desirableMetricForLoad(cpuThreshold);

              final double ratio = desirableEvents / (double) event.processedEvents;
              final int numExecutors = taskExecutorMap.keySet().size();
              final int offloadingCnt = Math.min(numExecutors, (int) Math.ceil(ratio * numExecutors));

              LOG.info("Desirable events: {} for load {}, total: {}, offloadingCnt: {}",
                desirableEvents, cpuThreshold, event.processedEvents, offloadingCnt);

              int cnt = 0;
              for (final TaskExecutor taskExecutor : taskExecutorMap.keySet()) {
                if (taskExecutor.isStateless()) {
                  LOG.info("Start offloading of {}", taskExecutor.getId());
                  taskExecutor.startOffloading(event.startTime);
                  prevOffloadingExecutors.add(taskExecutor);

                  cnt += 1;
                  if (offloadingCnt == cnt) {
                    break;
                  }
                }
              }
            }
            break;
          }

          case END: {
            if (started) {
              started = false;
              for (final TaskExecutor taskExecutor : prevOffloadingExecutors) {
                LOG.info("End offloading of {}", taskExecutor.getId());
                taskExecutor.endOffloading();
              }

              prevEndTime = System.currentTimeMillis();
              prevOffloadingExecutors.clear();
            }
            break;
          }
          default:
            throw new RuntimeException("Invalid state");
        }
      }
    }
  }
  */
}
