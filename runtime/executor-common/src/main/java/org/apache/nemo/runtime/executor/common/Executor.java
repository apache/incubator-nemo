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
package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.common.*;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.vertex.utility.StreamVertex;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.common.exception.IllegalMessageException;
import org.apache.nemo.common.exception.UnknownFailureCauseException;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.Task;
import org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage;
import org.apache.nemo.offloading.common.StateStore;
import org.apache.nemo.runtime.executor.common.datatransfer.PipeManagerWorker;
import org.apache.nemo.runtime.executor.common.datatransfer.IntermediateDataIOFactory;
import org.apache.nemo.runtime.executor.common.tasks.*;
import org.apache.nemo.runtime.message.*;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.nemo.runtime.executor.common.TaskExecutorUtil.getDecoderFactory;
import static org.apache.nemo.runtime.executor.common.TaskExecutorUtil.getEncoderFactory;
import static org.apache.nemo.runtime.message.MessageEnvironment.ListenerType.*;


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

  private final PersistentConnectionToMasterMap persistentConnectionToMasterMap;

  private final MetricMessageSender metricMessageSender;

  private volatile boolean started = false;

  private final TaskExecutorMapWrapper taskExecutorMapWrapper;
  private final ExecutorService executorService;

  private final EvalConf evalConf;
  // private final DeprecatedOffloadingWorkerFactory offloadingWorkerFactory;
  private final PipeManagerWorker pipeManagerWorker;

  private ScheduledExecutorService scheduledExecutorService;
  private final ExecutorService taskEventExecutorService;

  private final StageExecutorThreadMap stageExecutorThreadMap;

  private final AtomicInteger numReceivedTasks = new AtomicInteger(0);

  private final ExecutorService prepareService = Executors.newCachedThreadPool();

  final PipeIndexMapWorker  pipeIndexMapWorker;

  private final ExecutorThreads executorThreads;

  private final StateStore stateStore;

  private final ExecutorChannelManagerMap executorChannelManagerMap;

  private final TaskScheduledMapWorker scheduledMapWorker;

  // private final OffloadingManager offloadingManager;

  private final OutputCollectorGenerator outputCollectorGenerator;

  // private final NettyStateStore nettyStateStore;

  private final CpuBottleneckDetector bottleneckDetector;

  // private final DefaultOffloadingPreparer preparer;

  private final ExecutorMetrics executorMetrics;

  // private final OffloadingWorkerFactory workerFactory;

  private final MessageEnvironment messageEnvironment;

  private final StreamVertexSerializerManager streamVertexSerializerManager;

  private final DefaultCondRouting condRouting;

  private final boolean onLambda;

  @Inject
  private Executor(@Parameter(JobConf.ExecutorId.class) final String executorId,
                   @Parameter(JobConf.ExecutorResourceType.class) final String resourceType,
                   final PersistentConnectionToMasterMap persistentConnectionToMasterMap,
                   final MessageEnvironment messageEnvironment,
                   final SerializerManager serializerManager,
                   final IntermediateDataIOFactory intermediateDataIOFactory,
                   final MetricMessageSender metricMessageSender,
                   final ExecutorMetrics executorMetrics,
                   final CpuBottleneckDetector bottleneckDetector,
                   final EvalConf evalConf,
                   @Parameter(EvalConf.ExecutorOnLambda.class) final boolean onLambda,
                   // final SystemLoadProfiler profiler,
                   final PipeManagerWorker pipeManagerWorker,
                   final TaskExecutorMapWrapper taskExecutorMapWrapper,
                   final PipeIndexMapWorker pipeIndexMapWorker,
                   final StageExecutorThreadMap stageExecutorThreadMap,
                   // final JobScalingHandlerWorker jobScalingHandlerWorker,
                   final ExecutorThreads executorThreads,
                   final StreamVertexSerializerManager streamVertexSerializerManager,
                   // final ExecutorMetrics executorMetrics,
                   // final ScalingOutCounter scalingOutCounter,
                   // final SFTaskMetrics sfTaskMetrics,
                   final StateStore stateStore,
                   // final NettyStateStore nettyStateStore,
                   final ExecutorChannelManagerMap executorChannelManagerMap,
                   final TaskScheduledMapWorker taskScheduledMapWorker,
                   final CyclicDependencyHandler cyclicDependencyHandler,
                   final DefaultCondRouting condRouting,
                   // final OffloadingWorkerFactory workerFactory,
                   // final OffloadingManager offloadingManager,
                   final OutputCollectorGenerator outputCollectorGenerator) {
                   //@Parameter(EvalConf.BottleneckDetectionCpuThreshold.class) final double threshold,
                   //final CpuEventModel cpuEventModel) {

    this.onLambda = onLambda;

    this.messageEnvironment = messageEnvironment;
    this.condRouting = condRouting;
    this.streamVertexSerializerManager = streamVertexSerializerManager;
    this.executorMetrics = executorMetrics;
    this.bottleneckDetector = bottleneckDetector;
    bottleneckDetector.start();
    // this.nettyStateStore = nettyStateStore;
    // this.offloadingManager = offloadingManager;
    this.executorChannelManagerMap = executorChannelManagerMap;
    this.stateStore = (StateStore) stateStore;
    this.executorThreads = executorThreads;
    this.scheduledMapWorker = taskScheduledMapWorker;
    this.executorId = executorId;
    this.pipeManagerWorker = pipeManagerWorker;
    this.taskEventExecutorService = Executors.newSingleThreadExecutor();
    this.pipeIndexMapWorker = pipeIndexMapWorker;
    this.outputCollectorGenerator = outputCollectorGenerator;
    this.executorService = Executors.newCachedThreadPool();
    //this.executorService = Executors.newCachedThreadPool(new BasicThreadFactory.Builder()
    //          .namingPattern("TaskExecutor thread-%d")
    //          .build());
    this.persistentConnectionToMasterMap = persistentConnectionToMasterMap;
    this.serializerManager = serializerManager;
    this.intermediateDataIOFactory = intermediateDataIOFactory;
    this.metricMessageSender = metricMessageSender;
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    // this.preparer = preparer;
   //  this.workerFactory = workerFactory;

    scheduledExecutorService.scheduleAtFixedRate(() -> {
      if (resourceType.equals("Compute")) {
        CpuInfoExtractor.printNetworkStat(-1);
      }

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
    messageEnvironment.setupListener(EXECUTOR_MESSAGE_LISTENER_ID, new ExecutorMessageReceiver());

    taskScheduledMapWorker.init();
    executorChannelManagerMap.init();

    this.stageExecutorThreadMap = stageExecutorThreadMap;
  }

  public void start() {
    // Connect the globally known message listener IDs.
    try {
      messageEnvironment.<ControlMessage.Message>asyncConnect(MessageEnvironment.MASTER_ID,
        RUNTIME_MASTER_MESSAGE_LISTENER_ID).get();
      messageEnvironment.<ControlMessage.Message>asyncConnect(MessageEnvironment.MASTER_ID,
        BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID).get();
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    /*
    scheduledExecutorService.schedule(() -> {
      workerFactory.start();
    }, 5, TimeUnit.SECONDS);
    */
  }

  public String getExecutorId() {
    return executorId;
  }

  private synchronized void onTaskReceived(final Task task,
                                           final byte[] bytes) {
    LOG.info("Executor [{}] received Task [{}] to execute.",
        new Object[]{executorId, task.getTaskId()});

    final long st = System.currentTimeMillis();

    /*
    final ByteArrayInputStream bis = new ByteArrayInputStream(task.getSerializedIRDag());
    final DataInputStream dis = new DataInputStream(bis);
    final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag =
      DAG.decode(dis);
      */

    final long et = System.currentTimeMillis();

    LOG.info("Task {} irDag deser time {}", task.getTaskId(), et - st);

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

    try {
      final long s = System.currentTimeMillis();
      launchTask(task, bytes, task.getIrDag());
      LOG.info("Task launch time {} : time {}", task.getTaskId(), System.currentTimeMillis() - s);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException();
    }
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
                          final byte[] bytes,
                          final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag) {

    streamVertexSerializerManager.register(task);
    final long st = System.currentTimeMillis();

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

    /*
    irDag.getVertices().forEach(v -> {
      irDag.getOutgoingEdgesOf(v).forEach(e -> serializerManager.register(e.getId(),
        getEncoderFactory(e.getPropertyValue(EncoderProperty.class).get()),
        getDecoderFactory(e.getPropertyValue(DecoderProperty.class).get()),
        e.getPropertyValue(CompressionProperty.class).orElse(null),
        e.getPropertyValue(DecompressionProperty.class).orElse(null)));
    });
    */

    LOG.info("{} Launch task: {}, edge register time {}", executorId, task.getTaskId(), System.currentTimeMillis() - st);

    /*
    //LOG.info("Non-copied outgoing edges: {}", task.getTaskOutgoingEdges());
    final byte[] bytes = SerializationUtils.serialize((Serializable) task.getTaskOutgoingEdges());
    final List<StageEdge> copyOutgoingEdges = SerializationUtils.deserialize(bytes);
    //LOG.info("Copied outgoing edges {}, bytes: {}", copyOutgoingEdges, bytes.length);
    final byte[] bytes2 = SerializationUtils.serialize((Serializable) task.getTaskIncomingEdges());
    final List<StageEdge> copyIncomingEdges = SerializationUtils.deserialize(bytes2);
    */

    try {
      final int numTask = numReceivedTasks.getAndIncrement();
      final int index = numTask % evalConf.executorThreadNum;
      final ExecutorThread executorThread = executorThreads.getExecutorThreads().get(index);

      TaskExecutor taskExecutor;

      if (irDag.getVertices().size() == 1 && irDag.getVertices().get(0) instanceof StreamVertex) {
        taskExecutor = new StreamTaskExecutorImpl(
          Thread.currentThread().getId(),
          executorId,
          task,
          irDag,
          intermediateDataIOFactory,
          serializerManager,
          evalConf.samplingJson,
          prepareService,
          executorThread,
          pipeManagerWorker,
          stateStore,
          pipeManagerWorker,
          outputCollectorGenerator,
          bytes,
          // new NoOffloadingPreparer(),
          false);
      } else if (task.isCrTask()) {
        // conditional routing task
        taskExecutor =
          new CRTaskExecutorImpl(
            Thread.currentThread().getId(),
            executorId,
            task,
            irDag,
            intermediateDataIOFactory,
            serializerManager,
            null,
            evalConf.samplingJson,
            evalConf.isLocalSource,
            prepareService,
            executorThread,
            pipeManagerWorker,
            stateStore,
            // offloadingManager,
            pipeManagerWorker,
            outputCollectorGenerator,
            bytes,
            condRouting,
            // new NoOffloadingPreparer(),
            false);
      } else {
        taskExecutor =
          new DefaultTaskExecutorImpl(
            Thread.currentThread().getId(),
            executorId,
            task,
            irDag,
            intermediateDataIOFactory,
            serializerManager,
            null,
            evalConf.samplingJson,
            evalConf.isLocalSource,
            prepareService,
            executorThread,
            pipeManagerWorker,
            stateStore,
            // offloadingManager,
            pipeManagerWorker,
            outputCollectorGenerator,
            bytes,
            condRouting,
            // new NoOffloadingPreparer(),
            false);
      }

      LOG.info("Add Task {} to {} thread of {}, time {}", taskExecutor.getId(), index, executorId,
        System.currentTimeMillis() - st);

      executorThread.addNewTask(taskExecutor);

      LOG.info("Put Task time {} to {} thread of {}, time {}", taskExecutor.getId(), index, executorId,
        System.currentTimeMillis() - st);

    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
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
      // nettyStateStore.close();
      bottleneckDetector.close();

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
        case ThrottleSource: {
          LOG.info("Throttle source message for {}, rate {}", executorId, message.getSetNum());
          for (final TaskExecutor te : taskExecutorMapWrapper.getTaskExecutorMap().keySet()) {
            if (te.isSource()) {
              te.setThrottleSourceRate(message.getSetNum());
            }
          }
          break;
        }
        case CreateOffloadingExecutor: {
          throw new RuntimeException("Not supported");
//          LOG.info("Create offloadfing executor for {}", executorId);
//          executorService.execute(() -> {
//            offloadingManager.createWorker((int) message.getSetNum());
//          });
          // break;
        }
        case DeoffloadingTask: {
          final ControlMessage.OffloadingTaskMessage m = message.getOffloadingTaskMsg();
          int cnt = 0;
          final int stageId = (int) m.getOffloadingStage();

          for (final TaskExecutor te : taskExecutorMapWrapper.getTaskExecutorMap().keySet()) {
            if (cnt == m.getNumOffloadingTask()) {
              break;
            }

            if (te.getStatus().equals(DefaultTaskExecutorImpl.CurrentState.OFFLOADED)
              && RuntimeIdManager.getStageIdFromTaskId(te.getId()).equals("Stage" + stageId)) {
              LOG.info("Deoffloadfing task {} executor for {}", te.getId(), executorId);
              taskExecutorMapWrapper.getTaskExecutorThread(te.getId())
                .addShortcutEvent(new TaskOffloadingEvent(te.getId(),
                  TaskOffloadingEvent.ControlType.DEOFFLOADING, null));

              cnt += 1;
            }
          }
          break;
        }
        case FinishBursty: {
          for (final TaskExecutor te : taskExecutorMapWrapper.getTaskExecutorMap().keySet()) {
            if (te.getStatus().equals(DefaultTaskExecutorImpl.CurrentState.OFFLOADED)) {
              LOG.info("Deoffloadfing task {} executor for {}", te.getId(), executorId);
              taskExecutorMapWrapper.getTaskExecutorThread(te.getId())
                .addShortcutEvent(new TaskOffloadingEvent(te.getId(),
                  TaskOffloadingEvent.ControlType.FINISH_BURSTY_COMPUTATION, null));
            }
          }
          break;
        }
        case SendBursty: {
          LOG.info("Offloading bursty in {}", executorId);
          final ControlMessage.OffloadingTaskMessage m = message.getOffloadingTaskMsg();
          int cnt = 0;
          for (final TaskExecutor te : taskExecutorMapWrapper.getTaskExecutorMap().keySet()) {
            if (cnt == m.getNumOffloadingTask()) {
              break;
            }

            if (te.getStatus().equals(DefaultTaskExecutorImpl.CurrentState.RUNNING)) {
              final ExecutorThread executorThread = taskExecutorMapWrapper.getTaskExecutorThread(te.getId());

              LOG.info("Add offloading task shortcut for task {} in {}", te.getId(), executorId);

              executorThread.addShortcutEvent(
                new TaskOffloadingEvent(te.getId(),
                  TaskOffloadingEvent.ControlType.SEND_BURSTY_COMPUTATION, null));

              cnt += 1;
            }
          }
          break;
        }
        case OffloadingTask: {
          LOG.info("Offloading task in {}", executorId);
          final ControlMessage.OffloadingTaskMessage m = message.getOffloadingTaskMsg();
          int cnt = 0;
          final int stageId = (int) m.getOffloadingStage();
          for (final TaskExecutor te : taskExecutorMapWrapper.getTaskExecutorMap().keySet()) {
            if (cnt == m.getNumOffloadingTask()) {
              break;
            }

            if (te.getStatus().equals(DefaultTaskExecutorImpl.CurrentState.RUNNING)
              && RuntimeIdManager.getStageIdFromTaskId(te.getId()).equals("Stage" + stageId)) {
              final ExecutorThread executorThread = taskExecutorMapWrapper.getTaskExecutorThread(te.getId());

              LOG.info("Add offloading task shortcut for task {} in {}", te.getId(), executorId);

              executorThread.addShortcutEvent(
                new TaskOffloadingEvent(te.getId(),
                  TaskOffloadingEvent.ControlType.SEND_TO_OFFLOADING_WORKER, null));

              cnt += 1;
            }
          }

          break;
        }
        case TaskScheduled: {
          executorService.execute(() -> {
            LOG.info("Task scheduled {} received at {}", message.getRegisteredExecutor(), executorId);
            final String[] split = message.getRegisteredExecutor().split(",");
            final String scheduledTaskId = split[0];

            scheduledMapWorker.registerTask(split[0], split[1]);

            taskExecutorMapWrapper.forEach(taskExecutor -> {
              final ExecutorThread executorThread = taskExecutorMapWrapper.getTaskExecutorThread(taskExecutor.getId());
              executorThread.addEvent(new TaskControlMessage(
                TaskControlMessage.TaskControlMessageType.TASK_SCHEDULED, -1, -1,
                taskExecutor.getId(), scheduledTaskId));
            });
          });
          break;
        }
        case RoutingDataToLambda: {
          // for R2 reshaping
          final String taskId = message.getStopTaskMsg().getTaskId();
          final Task task = taskExecutorMapWrapper.getTaskExecutor(taskId).getTask();

          if (!(task.isVMTask() || task.isTransientTask())) {
            throw new RuntimeException("Cannot stage migration task " + taskId + ", " + task.getTaskType());
          }

          executorService.execute(() -> {
            if (task.isVMTask()) {
              LOG.info("Routing data from VM task {} to Lambda  {} in executor {}",
                message.getStopTaskMsg().getTaskId(), task.getPairTaskId(), executorId);
            } else {
               LOG.info("Routing data from lambda task {} VM Lambda  {} in executor {}",
                message.getStopTaskMsg().getTaskId(), task.getPairTaskId(), executorId);
            }
            final ExecutorThread executorThread = taskExecutorMapWrapper.getTaskExecutorThread(message.getStopTaskMsg().getTaskId());
            executorThread.addShortcutEvent(new TaskControlMessage(
              TaskControlMessage.TaskControlMessageType.INVOKE_REDIRECTION_FOR_CR, -1, -1,
              message.getStopTaskMsg().getTaskId(), false));
          });
          break;
        }
        case DeactivateLambdaTask: {
          // for R2 reshaping
          LOG.info("Deactivation task in executor {}", executorId);

          taskExecutorMapWrapper.getTaskExecutorMap()
            .keySet().forEach(taskExecutor -> {
              executorService.execute(() -> {
                final ExecutorThread executorThread = taskExecutorMapWrapper.getTaskExecutorThread(taskExecutor.getId());
                executorThread.addEvent(new TaskControlMessage(
                  TaskControlMessage.TaskControlMessageType.INVOKE_REDIRECTION_FOR_CR, -1, -1,
                  taskExecutor.getId(), true));
              });
            });
          break;
        }
        case GetStateSignal: {
          // for R2 reshaping
          final String taskId = message.getStopTaskMsg().getTaskId();
          final Task task = taskExecutorMapWrapper.getTaskExecutor(taskId).getTask();
          executorService.execute(() -> {
            LOG.info("Get state signal from VM task {} to Lambda {} in executor {}",
              task.getPairTaskId(), message.getStopTaskMsg().getTaskId(), executorId);

            final ExecutorThread executorThread = taskExecutorMapWrapper.getTaskExecutorThread(message.getStopTaskMsg().getTaskId());
            executorThread.addShortcutEvent(new TaskControlMessage(
              TaskControlMessage.TaskControlMessageType.GET_STATE_SIGNAL, -1, -1,
              message.getStopTaskMsg().getTaskId(), null));
          });

          break;
        }
        case TaskOutputStart: {
          // for R2 reshaping
          final String taskId = message.getStopTaskMsg().getTaskId();
          final Task task = taskExecutorMapWrapper.getTaskExecutor(taskId).getTask();
          executorService.execute(() -> {
            LOG.info("Get init signal VM task {} in executor {}",
              message.getStopTaskMsg().getTaskId(), executorId);

            final ExecutorThread executorThread = taskExecutorMapWrapper.getTaskExecutorThread(message.getStopTaskMsg().getTaskId());
            executorThread.addShortcutEvent(new TaskControlMessage(
              TaskControlMessage.TaskControlMessageType.TASK_OUTPUT_START, -1, -1,
              message.getStopTaskMsg().getTaskId(), null));
          });

          break;
        }
        case StopTask: {
          // TODO: receive stop task message
          executorService.execute(() -> {
            LOG.info("Stopping task {} in executor {}", message.getStopTaskMsg().getTaskId(), executorId);
            final ExecutorThread executorThread = taskExecutorMapWrapper.getTaskExecutorThread(message.getStopTaskMsg().getTaskId());
            executorThread.addShortcutEvent(new TaskControlMessage(
              TaskControlMessage.TaskControlMessageType.TASK_STOP_SIGNAL_BY_MASTER, -1, -1,
              message.getStopTaskMsg().getTaskId(), null));
          });
          break;
        }
        case ExecutorRegistered: {
          executorService.execute(() -> {
            LOG.info("Executor registered message received {}", message.getRegisteredExecutor());
            final String[] registeredExecutors = message.getRegisteredExecutor().split(",");
            for (int i = 0; i < registeredExecutors.length; i++) {
              executorChannelManagerMap.initConnectToExecutor(registeredExecutors[i]);
            }
          });
          break;
        }
        case ExecutorRemoved: {
          executorService.execute(() -> {
            LOG.info("Executor removed message received {}", message.getRegisteredExecutor());
            executorChannelManagerMap.removeExecutor(message.getRegisteredExecutor());
          });
          break;
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
          break;
        case ScheduleTask:
          executorService.execute(() -> {
            final long st = System.currentTimeMillis();
            final ControlMessage.ScheduleTaskMsg scheduleTaskMsg = message.getScheduleTaskMsg();
            final byte[] bytes = scheduleTaskMsg.getTask().toByteArray();
            final ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            final DataInputStream dis = new DataInputStream(bis);
            final Task task = Task.decode(dis);

            executorMetrics.taskInputProcessRateMap
              .put(task.getTaskId(), Pair.of(new AtomicLong(), new AtomicLong()));

            LOG.info("Task {} received in executor {}, serialized time {}", task.getTaskId(), executorId, System.currentTimeMillis() - st);
            onTaskReceived(task, bytes);
          });
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
  final class BottleneckHandler implements EventHandler<CpuBottleneckDetectorImpl.BottleneckEvent> {

    private List<TaskExecutor> prevOffloadingExecutors = new ArrayList<>();
    private long prevEndTime = System.currentTimeMillis();
    private long slackTime = 10000;
    private boolean started = false;

    @Override
    public void onNext(final CpuBottleneckDetectorImpl.BottleneckEvent event) {
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
                  LOG.info("Start prepareOffloading of {}", taskExecutor.getId());
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
                LOG.info("End prepareOffloading of {}", taskExecutor.getId());
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
