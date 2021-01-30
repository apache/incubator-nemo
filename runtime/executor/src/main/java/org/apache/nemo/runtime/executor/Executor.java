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
import org.apache.log4j.Level;
import org.apache.nemo.common.*;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.conf.EvalConf;
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
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageContext;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.MessageListener;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.Task;
import org.apache.nemo.runtime.common.state.TaskState;
import org.apache.nemo.runtime.executor.bytetransfer.ByteTransport;
import org.apache.nemo.runtime.executor.common.*;
import org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage;
import org.apache.nemo.common.StateStore;
import org.apache.nemo.runtime.executor.data.CyclicDependencyHandler;
import org.apache.nemo.runtime.executor.common.datatransfer.PipeManagerWorker;
import org.apache.nemo.runtime.executor.data.SerializerManager;
import org.apache.nemo.runtime.executor.datatransfer.IntermediateDataIOFactory;
import org.apache.nemo.runtime.executor.data.BroadcastManagerWorker;
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
import java.util.stream.Collectors;

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


  private volatile boolean started = false;

  private final TaskExecutorMapWrapper taskExecutorMapWrapper;
  private final ExecutorService executorService;

  private final EvalConf evalConf;
  // private final OffloadingWorkerFactory offloadingWorkerFactory;

  private final ByteTransport byteTransport;
  private final PipeManagerWorker pipeManagerWorker;

  private ScheduledExecutorService scheduledExecutorService;
  private final ExecutorService taskEventExecutorService;

  private final StageExecutorThreadMap stageExecutorThreadMap;

  private final AtomicInteger numReceivedTasks = new AtomicInteger(0);

  private final ExecutorService prepareService = Executors.newCachedThreadPool();

  final PipeIndexMapWorker taskTransferIndexMap;

  private final ExecutorThreads executorThreads;

  private final StateStore stateStore;

  private final ExecutorContextManagerMap executorContextManagerMap;

  private final TaskScheduledMapWorker scheduledMapWorker;

  private final ControlEventHandler controlEventHandler;

  @Inject
  private Executor(@Parameter(JobConf.ExecutorId.class) final String executorId,
                   final PersistentConnectionToMasterMap persistentConnectionToMasterMap,
                   final MessageEnvironment messageEnvironment,
                   final SerializerManager serializerManager,
                   final IntermediateDataIOFactory intermediateDataIOFactory,
                   final BroadcastManagerWorker broadcastManagerWorker,
                   final MetricManagerWorker metricMessageSender,
                   final ByteTransport byteTransport,
                   //final CpuBottleneckDetector bottleneckDetector,
                   // final OffloadingWorkerFactory offloadingWorkerFactory,
                   final EvalConf evalConf,
                   // final SystemLoadProfiler profiler,
                   final PipeManagerWorker pipeManagerWorker,
                   final TaskExecutorMapWrapper taskExecutorMapWrapper,
                   final PipeIndexMapWorker taskTransferIndexMap,
                   // final RelayServer relayServer,
                   final StageExecutorThreadMap stageExecutorThreadMap,
                   // final JobScalingHandlerWorker jobScalingHandlerWorker,
                   final ExecutorThreads executorThreads,
                   // final ExecutorMetrics executorMetrics,
                   // final ScalingOutCounter scalingOutCounter,
                   // final SFTaskMetrics sfTaskMetrics,
                   final StateStore stateStore,
                   final ControlEventHandler controlEventHandler,
                   final ExecutorContextManagerMap executorContextManagerMap,
                   final TaskScheduledMapWorker taskScheduledMapWorker,
                   final CyclicDependencyHandler cyclicDependencyHandler) {
                   //@Parameter(EvalConf.BottleneckDetectionCpuThreshold.class) final double threshold,
                   //final CpuEventModel cpuEventModel) {
    org.apache.log4j.Logger.getLogger(org.apache.kafka.clients.consumer.internals.Fetcher.class).setLevel(Level.WARN);
    org.apache.log4j.Logger.getLogger(org.apache.kafka.clients.consumer.ConsumerConfig.class).setLevel(Level.WARN);

    this.executorContextManagerMap = executorContextManagerMap;
    this.stateStore = (StateStore) stateStore;
    this.executorThreads = executorThreads;
    this.scheduledMapWorker = taskScheduledMapWorker;
    this.executorId = executorId;
    this.byteTransport = byteTransport;
    this.controlEventHandler = controlEventHandler;
    this.pipeManagerWorker = pipeManagerWorker;
    this.taskEventExecutorService = Executors.newSingleThreadExecutor();
    this.taskTransferIndexMap = taskTransferIndexMap;
    this.executorService = Executors.newCachedThreadPool();
    //this.executorService = Executors.newCachedThreadPool(new BasicThreadFactory.Builder()
    //          .namingPattern("TaskExecutor thread-%d")
    //          .build());
    this.persistentConnectionToMasterMap = persistentConnectionToMasterMap;
    this.serializerManager = serializerManager;
    this.intermediateDataIOFactory = intermediateDataIOFactory;
    this.broadcastManagerWorker = broadcastManagerWorker;
    this.metricMessageSender = metricMessageSender;
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    scheduledExecutorService.scheduleAtFixedRate(() -> {
      // final double load = profiler.getCpuLoad();
      // LOG.info("Cpu load: {}", load);

      /*
      if (isThrottleTime(load)) {
        // Send input
        if (!Throttled.getInstance().getThrottled()) {
          LOG.info("Set throttled true : load {}", load);
          Throttled.getInstance().setThrottle(true);
        }
      }

      if (isUnthrottleTime(load)) {
        if (Throttled.getInstance().getThrottled()) {
          LOG.info("Set throttled false: load {}", load);
          Throttled.getInstance().setThrottle(false);
        }
      }
      */

      /*
      // Send task stats
      final Set<TaskExecutor> taskExecutors = taskExecutorMapWrapper.getTaskExecutorMap().keySet();

      final List<ControlMessage.TaskStatInfo> taskStatInfos = taskExecutors.stream().map(taskExecutor -> {

        final String taskId = taskExecutor.getId();

        if (taskLocationMap.locationMap.get(taskId) == SF
          || taskLocationMap.locationMap.get(taskId) == VM_SCALING) {
          // get metric from SF
          if (sfTaskMetrics.sfTaskMetrics.containsKey(taskId)) {
            final TaskMetrics.RetrievedMetrics metric = sfTaskMetrics.sfTaskMetrics.get(taskId);
            return ControlMessage.TaskStatInfo.newBuilder()
              .setNumKeys(metric.numKeys)
              .setTaskId(taskExecutor.getId())
              .setInputElements(metric.inputElement)
              .setOutputElements(metric.outputElement)
              .setComputation(metric.computation)
              .build();
          } else {
            // 걍 기존 metric 보내줌
            final TaskMetrics.RetrievedMetrics retrievedMetrics =
              taskExecutor.getTaskMetrics().retrieve(taskExecutor.getNumKeys());
            return ControlMessage.TaskStatInfo.newBuilder()
              .setNumKeys(taskExecutor.getNumKeys())
              .setTaskId(taskExecutor.getId())
              .setInputElements(retrievedMetrics.inputElement)
              .setOutputElements(retrievedMetrics.outputElement)
              .setComputation(retrievedMetrics.computation)
              .build();
          }
        } else {
          final TaskMetrics.RetrievedMetrics retrievedMetrics =
            taskExecutor.getTaskMetrics().retrieve(taskExecutor.getNumKeys());
          return ControlMessage.TaskStatInfo.newBuilder()
            .setNumKeys(taskExecutor.getNumKeys())
            .setTaskId(taskExecutor.getId())
            .setInputElements(retrievedMetrics.inputElement)
            .setOutputElements(retrievedMetrics.outputElement)
            .setComputation(retrievedMetrics.computation)
            .build();
        }
      }).collect(Collectors.toList());


      final long sfComputation =
        taskStatInfos.stream().filter(taskStatInfo -> {
        return taskLocationMap.locationMap.get(taskStatInfo.getTaskId()) == SF
          || taskLocationMap.locationMap.get(taskStatInfo.getTaskId()) == VM_SCALING;
      }).map(taskStatInfo -> taskStatInfo.getComputation())
        .reduce(0L, (x, y) -> x + y);

      final long vmComputation =
        Math.max(700000,
        taskStatInfos.stream().filter(taskStatInfo -> {
        return taskLocationMap.locationMap.get(taskStatInfo.getTaskId()) == VM;
      }).map(taskStatInfo -> taskStatInfo.getComputation())
        .reduce(0L, (x, y) -> x + y));

      final double sfCpuLoad = ((sfComputation  / (double)vmComputation) * Math.max(0.1, (load - 0.2))) / 1.8;

      //final double sfCpuLoad = sfTaskMetrics.cpuLoadMap.values().stream().reduce(0.0, (x, y) -> x + y);

      LOG.info("VM cpu use: {}, SF cpu use: {}", load, sfCpuLoad);

      persistentConnectionToMasterMap.getMessageSender(SCALE_DECISION_MESSAGE_LISTENER_ID)
        .send(ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(SCALE_DECISION_MESSAGE_LISTENER_ID)
          .setType(ControlMessage.MessageType.TaskStatSignal)
          .setTaskStatMsg(ControlMessage.TaskStatMessage.newBuilder()
            .setExecutorId(executorId)
            .addAllTaskStats(taskStatInfos)
            .setCpuUse(load)
            .setSfCpuUse(sfCpuLoad)
            .build())
          .build());
          */

    }, 1, 1, TimeUnit.SECONDS);


    // relayServer address/port 보내기!!
    /*
    LOG.info("Sending local relay server info: {}/{}/{}",
      executorId, relayServer.getPublicAddress(), relayServer.getPort());

    persistentConnectionToMasterMap
      .getMessageSender(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID).send(
      ControlMessage.Message.newBuilder()
        .setId(RuntimeIdManager.generateMessageId())
        .setListenerId(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID)
        .setType(ControlMessage.MessageType.LocalRelayServerInfo)
        .setLocalRelayServerInfoMsg(ControlMessage.LocalRelayServerInfoMessage.newBuilder()
          .setExecutorId(executorId)
          .setAddress(relayServer.getPublicAddress())
          .setPort(relayServer.getPort())
          .build())
        .build());
    */

    this.evalConf = evalConf;
    LOG.info("\n{}", evalConf);
    // this.offloadingWorkerFactory = offloadingWorkerFactory;
    this.taskExecutorMapWrapper = taskExecutorMapWrapper;
    messageEnvironment.setupListener(MessageEnvironment.EXECUTOR_MESSAGE_LISTENER_ID, new ExecutorMessageReceiver());

    taskScheduledMapWorker.init();
    executorContextManagerMap.init();

    this.stageExecutorThreadMap = stageExecutorThreadMap;
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
        // taskOffloader.start();
      }

      if (evalConf.offloadingdebug) {
         // taskOffloader.startDownstreamDebugging();
        //taskOffloader.startDebugging();
      }

      //bottleneckDetector.setBottleneckHandler(new BottleneckHandler());
      //bottleneckDetector.start();
      started = true;
    }

    executorService.execute(() -> {
    try {
      launchTask(task, irDag);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException();
    }});
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

    LOG.info("{} Launch task: {}", executorId, task.getTaskId());

    //LOG.info("Non-copied outgoing edges: {}", task.getTaskOutgoingEdges());
    final byte[] bytes = SerializationUtils.serialize((Serializable) task.getTaskOutgoingEdges());
    final List<StageEdge> copyOutgoingEdges = SerializationUtils.deserialize(bytes);
    //LOG.info("Copied outgoing edges {}, bytes: {}", copyOutgoingEdges, bytes.length);
    final byte[] bytes2 = SerializationUtils.serialize((Serializable) task.getTaskIncomingEdges());
    final List<StageEdge> copyIncomingEdges = SerializationUtils.deserialize(bytes2);

    try {
      final TaskStateManager taskStateManager =
          new TaskStateManager(task, executorId, persistentConnectionToMasterMap, metricMessageSender);

      final int numTask = numReceivedTasks.getAndIncrement();
      final int index = numTask % evalConf.executorThreadNum;
      final ExecutorThread executorThread = executorThreads.getExecutorThreads().get(index);


      final TaskExecutor taskExecutor =
      new DefaultTaskExecutorImpl(
        Thread.currentThread().getId(),
        executorId,
        task,
        irDag,
        intermediateDataIOFactory,
        broadcastManagerWorker,
        metricMessageSender,
        persistentConnectionToMasterMap,
        serializerManager,
        null,
        evalConf,
        prepareService,
        executorThread,
        pipeManagerWorker,
        stateStore);

      LOG.info("Add Task {} to {} thread of {}", taskExecutor.getId(), index, executorId);
      executorThreads.getExecutorThreads().get(index).addNewTask(taskExecutor);
      taskExecutorMapWrapper.putTaskExecutor(taskExecutor, executorThreads.getExecutorThreads().get(index));

      //taskExecutor.execute();
      taskStateManager.onTaskStateChanged(TaskState.State.EXECUTING, Optional.empty(), Optional.empty());

      persistentConnectionToMasterMap.getMessageSender(MessageEnvironment.TASK_SCHEDULE_MAP_LISTENER_ID)
        .send(ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(MessageEnvironment.TASK_SCHEDULE_MAP_LISTENER_ID)
          .setType(ControlMessage.MessageType.TaskExecuting)
        .setTaskExecutingMsg(ControlMessage.TaskExecutingMessage.newBuilder()
          .setExecutorId(executorId)
          .setTaskId(task.getTaskId())
          .build())
          .build());

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

      e.printStackTrace();
      throw new RuntimeException(e);
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
      for (final Pair<AtomicInteger, List<ExecutorThread>> pair :
        stageExecutorThreadMap.getStageExecutorThreadMap().values()) {
        pair.right().forEach(ExecutorThread::close);
      }
      scheduledExecutorService.shutdownNow();
      executorService.shutdownNow();
      taskEventExecutorService.shutdownNow();
      prepareService.shutdownNow();

      for (final ExecutorThread t : executorThreads.getExecutorThreads()) {
        t.close();
      }

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
    public synchronized void onMessage(final ControlMessage.Message message) {
      switch (message.getType()) {
        case TaskScheduled: {
          LOG.info("Task scheduled {} received at {}", message.getRegisteredExecutor(), executorId);
          final String[] split = message.getRegisteredExecutor().split(",");
          scheduledMapWorker.registerTask(split[0], split[1]);
          pipeManagerWorker.taskScheduled(split[0]);
          break;
        }
        case StopTask: {
          // TODO: receive stop task message
          LOG.info("Stopping task {} in executor {}", message.getStopTaskMsg().getTaskId(), executorId);
          controlEventHandler.handleControlEvent(new TaskControlMessage(
            TaskControlMessage.TaskControlMessageType.TASK_STOP_SIGNAL_BY_MASTER, -1, -1,
            message.getStopTaskMsg().getTaskId(), null));
          break;
        }
        case ExecutorRegistered: {
          LOG.info("Executor registered message received {}", message.getRegisteredExecutor());
          executorContextManagerMap.initConnectToExecutor(message.getRegisteredExecutor());
          break;
        }
        case ExecutorRemoved: {
          LOG.info("Executor removed message received {}", message.getRegisteredExecutor());
          executorContextManagerMap.removeExecutor(message.getRegisteredExecutor());
          break;
        }
        case GlobalExecutorAddressInfo: {
          throw new RuntimeException("hahahah GlobalExecutorAddressInfo.. what?");
          /*
          final ControlMessage.GlobalExecutorAddressInfoMessage msg = message.getGlobalExecutorAddressInfoMsg();

          final Map<String, Pair<String, Integer>> m =
            msg.getInfosList()
              .stream()
              .collect(Collectors.toMap(ControlMessage.LocalExecutorAddressInfoMessage::getExecutorId,
                entry -> {
                  return Pair.of(entry.getAddress(), entry.getPort());
                }));

          LOG.info("{} Setting global executor address server info {}", executorId, m);

          byteTransport.setExecutorAddressMap(m);
          */

          /*
          if (offloadingWorkerFactory instanceof VMOffloadingWorkerFactory) {
            LOG.info("Set vm addresses");
            final VMOffloadingWorkerFactory vmOffloadingWorkerFactory = (VMOffloadingWorkerFactory) offloadingWorkerFactory;
            vmOffloadingWorkerFactory.setVMAddressAndIds(msg.getVmAddressesList(), msg.getVmIdsList());
          }
          */
        }
        case GlobalRelayServerInfo:

          final ControlMessage.GlobalRelayServerInfoMessage msg = message.getGlobalRelayServerInfoMsg();

          final Map<String, Pair<String, Integer>> m =
            msg.getInfosList()
              .stream()
              .collect(Collectors.toMap(ControlMessage.LocalRelayServerInfoMessage::getExecutorId,
                entry -> {
                  return Pair.of(entry.getAddress(), entry.getPort());
                }));

          // rendevousServerClient = new RendevousServerClient(msg.getRendevousAddress(), msg.getRendevousPort());

          LOG.info("{} Setting global relay server info {}", executorId, m);

          /*
          final OffloadingTransform lambdaExecutor = new OffloadingExecutor(
            evalConf.offExecutorThreadNum,
            byteTransport.getExecutorAddressMap(),
            serializerManager.runtimeEdgeIdToSerializer,
            // pipeManagerWorker.getTaskExecutorIdMap(),
            // taskTransferIndexMap.getMap(),
            relayServer.getPublicAddress(),
            relayServer.getPort(),
            msg.getRendevousAddress(),
            msg.getRendevousPort(),
            executorId,
            m,
            evalConf.offloadingType.equals("vm") ? VM_SCALING : SF);

          tinyWorkerManager = new TinyTaskOffloadingWorkerManager(
            offloadingWorkerFactory,
            lambdaExecutor,
            evalConf,
            sfTaskMetrics);

          jobScalingHandlerWorker.setTinyWorkerManager(tinyWorkerManager);
          */
          break;
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
