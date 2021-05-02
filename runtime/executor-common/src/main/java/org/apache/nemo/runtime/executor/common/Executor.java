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
import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;
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
import org.apache.nemo.runtime.executor.common.monitoring.AlarmManager;
import org.apache.nemo.runtime.executor.common.monitoring.SystemLoadProfiler;
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

  private final TaskScheduledMapWorker taskScheduledMapWorker;

  private final Map<String, Task> prevScheduledTaskCacheMap;

  private boolean activated = true;

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
                   final PipeManagerWorker pipeManagerWorker,
                   final TaskExecutorMapWrapper taskExecutorMapWrapper,
                   final PipeIndexMapWorker pipeIndexMapWorker,
                   final StageExecutorThreadMap stageExecutorThreadMap,
                   final ExecutorThreads executorThreads,
                   final StreamVertexSerializerManager streamVertexSerializerManager,
                   final StateStore stateStore,
                   // final NettyStateStore nettyStateStore,
                   final ExecutorChannelManagerMap executorChannelManagerMap,
                   final TaskScheduledMapWorker taskScheduledMapWorker,
                   final CyclicDependencyHandler cyclicDependencyHandler,
                   final DefaultCondRouting condRouting,
                   final SystemLoadProfiler profiler,
                   final OutputCollectorGenerator outputCollectorGenerator) {
                   //@Parameter(EvalConf.BottleneckDetectionCpuThreshold.class) final double threshold,
                   //final CpuEventModel cpuEventModel) {

    this.prevScheduledTaskCacheMap = new ConcurrentHashMap<>();
    this.onLambda = onLambda;
    this.taskScheduledMapWorker = taskScheduledMapWorker;
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
      if (activated) {
        if (!resourceType.equals("Source")) {
          CpuInfoExtractor.printNetworkStat(-1);
        }
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

    final long windowSize = 1000;
    final long delay = Util.THROTTLE_WINDOW;
    final long period = Util.THROTTLE_WINDOW;
    final AtomicLong prevSendTime = new AtomicLong(System.currentTimeMillis());
    final Map<String, Pair<Long, Long>> prevCountMap = new HashMap<>();
    final Map<String, List<Long>> processingCntHistory = new HashMap<>();
    final AtomicLong prevReceiveCnt = new AtomicLong(0);
    final AtomicLong prevProcessingCnt = new AtomicLong(0);
    final AtomicLong prevProcessingTime = new AtomicLong(0);

    final List<Pair<Long, Long>> processingCntHistory2 = new LinkedList<>();

    final AtomicLong throttleDelay = new AtomicLong((long) (delay * 0.7));

    if (resourceType.equals(ResourcePriorityProperty.SOURCE)) {
      // source event
      scheduledExecutorService.scheduleAtFixedRate(() -> {

        final long sourceEvent = taskExecutorMapWrapper.getTaskExecutorMap().keySet()
          .stream()
          .filter(te -> te.getTask().isSourceTask())
          .map(te -> te.getTaskMetrics().getInputProcessElement())
          .reduce((x, y) -> x + y)
          .orElse(0L);

        LOG.info("Source event {}", sourceEvent);

        persistentConnectionToMasterMap
          .getMessageSender(SOURCE_EVENT_HANDLER_ID).send(
          ControlMessage.Message.newBuilder()
            .setId(RuntimeIdManager.generateMessageId())
            .setListenerId(SOURCE_EVENT_HANDLER_ID.ordinal())
            .setType(ControlMessage.MessageType.SourceEvent)
            .setRegisteredExecutor(executorId)
            .setSetNum(sourceEvent)
            .build());
      }, 1, 1, TimeUnit.SECONDS);

    } else {
      scheduledExecutorService.scheduleAtFixedRate(() -> {
        try {
          // Send signal to source executor
            final long processCnt = executorMetrics.inputProcessCntMap.values().stream().reduce((x, y) -> x + y).get();
            final long receiveCnt = executorMetrics.inputReceiveCntMap.values().stream()
              .map(l -> l.get()).reduce((x, y) ->
                x + y).get();

            final long queueLength = receiveCnt - processCnt;
            final long prevQueueLength = prevReceiveCnt.get() - prevProcessingCnt.get();

            LOG.info("Send throttle event 2 from {} to source, delay {}, " +
                "queueLength: {} " +
                "input: {}, process: {}", executorId, 400,
              queueLength,
              receiveCnt, processCnt);

          if (activated) {
            final double cpuUse = profiler.getAvgCpuLoad();

            persistentConnectionToMasterMap
              .getMessageSender(SCALE_DECISION_MESSAGE_LISTENER_ID).send(
              ControlMessage.Message.newBuilder()
                .setId(RuntimeIdManager.generateMessageId())
                .setListenerId(SCALE_DECISION_MESSAGE_LISTENER_ID.ordinal())
                .setType(ControlMessage.MessageType.ExecutorMetric)
                .setExecutorMetricMsg(ControlMessage.ExecutorMetricMsg.newBuilder()
                  .setExecutorId(executorId)
                  .setReceiveEvent(receiveCnt)
                  .setProcessEvent(processCnt)
                  .setCpuUse(Math.min(1.0, cpuUse))
                  .build())
                .build());
          }

          /*
          if (queueLength > 30000) {

            LOG.info("Send throttle event 2 from {} to source, delay {}, " +
                "queueLength: {} " +
                "input: {}, process: {}", executorId, 400,
              queueLength,
              receiveCnt, processCnt);
            persistentConnectionToMasterMap
              .getMessageSender(RUNTIME_MASTER_MESSAGE_LISTENER_ID).send(
              ControlMessage.Message.newBuilder()
                .setId(RuntimeIdManager.generateMessageId())
                .setListenerId(RUNTIME_MASTER_MESSAGE_LISTENER_ID.ordinal())
                .setType(ControlMessage.MessageType.SourceSleep)
                .setSetNum(400)
                .build());
          }
          */

          prevSendTime.set(System.currentTimeMillis());
          prevReceiveCnt.set(receiveCnt);
          prevProcessingCnt.set(processCnt);



          /*
          final long processCnt = executorMetrics.inputProcessCntMap.values().stream().reduce((x, y) -> x + y).get();
          final long receiveCnt = executorMetrics.inputReceiveCntMap.values().stream()
            .map(l -> l.get()).reduce((x, y) ->
              x + y).get();

          final Optional<Long> processingTimeMsOpt = taskExecutorMapWrapper.getTaskExecutorMap().keySet()
            .stream().map(executor -> executor.getTaskMetrics().getComputation())
            .reduce((x, y) -> x / 1000 + y / 1000);

          if (!processingTimeMsOpt.isPresent() || processCnt  > receiveCnt) {
            LOG.info("No throttle receive cnt: {}, process cnt: {}", receiveCnt, processCnt);
            return;
          }

          if (processingCntHistory2.size() > 0) {
            final long processingTimeMs = processingTimeMsOpt.get();

            final double elapsed = (processingTimeMs - processingCntHistory2.get(0).left()) * 0.001;
            final double processRate;
            if (elapsed > 0) {
              processRate = (processCnt - processingCntHistory2.get(0).right()) / elapsed;
            } else {
              processRate = 0;
            }

            final long queueLength = receiveCnt - processCnt;
            final long prevQueueLength = prevReceiveCnt.get() - prevProcessingCnt.get();

            final double timeToProcessRemain;
            if (processRate == 0) {
              timeToProcessRemain = 0;
            } else {
              timeToProcessRemain = queueLength / (double) processRate;
            }

            if (timeToProcessRemain > 0.1) {
              // final int throttleDelay = (int) (delay * 5);
              // final int throttleDelay = (int) (0.8 * timeToProcessRemain * 1000);
              LOG.info("Send throttle event 11 from {} to source, delay {}, " +
                  "processRate: {}, queueLength: {}, processingTime: {}, elapsed: {}, " +
                  "input: {}, process: {}", executorId, throttleDelay,
                processRate, queueLength, processingTimeMs, elapsed,
                receiveCnt, processCnt);

              persistentConnectionToMasterMap
                .getMessageSender(RUNTIME_MASTER_MESSAGE_LISTENER_ID).send(
                ControlMessage.Message.newBuilder()
                  .setId(RuntimeIdManager.generateMessageId())
                  .setListenerId(RUNTIME_MASTER_MESSAGE_LISTENER_ID.ordinal())
                  .setType(ControlMessage.MessageType.SourceSleep)
                  .setSetNum(throttleDelay)
                  .build());
            } else if (queueLength > 10000) {
              LOG.info("Send throttle event 2 from {} to source, delay {}, " +
                  "processRate: {}, queueLength: {}, processingTime: {}, elapsed: {}, " +
                  "input: {}, process: {}", executorId, throttleDelay,
                processRate, queueLength, processingTimeMs, elapsed,
                receiveCnt, processCnt);
              persistentConnectionToMasterMap
                .getMessageSender(RUNTIME_MASTER_MESSAGE_LISTENER_ID).send(
                ControlMessage.Message.newBuilder()
                  .setId(RuntimeIdManager.generateMessageId())
                  .setListenerId(RUNTIME_MASTER_MESSAGE_LISTENER_ID.ordinal())
                  .setType(ControlMessage.MessageType.SourceSleep)
                  .setSetNum(throttleDelay)
                  .build());
            } else {
              LOG.info("No throttle event from {} to source, delay {}, " +
                  "processRate: {}, queueLength: {}, prevQueueLength: {}, processTime: {}, elapsed: {}," +
                  "input: {}, process: {}", executorId, throttleDelay,
                processRate, queueLength, prevQueueLength, processingTimeMs, elapsed,
                receiveCnt, processCnt);
            }
          }

          prevSendTime.set(System.currentTimeMillis());
          prevReceiveCnt.set(receiveCnt);
          prevProcessingCnt.set(processCnt);
          prevProcessingTime.set(processingTimeMsOpt.get());
          processingCntHistory2.add(Pair.of(processingTimeMsOpt.get(), processCnt));

          if (delay * processingCntHistory2.size() >= windowSize) {
            processingCntHistory2.remove(0);
          }
          */

        /*

        final Map<String, Integer> throttleDstTaskMaxDelayMap = new HashMap<>();
        final Map<String, Triple<String, String, String>> throttleDstTaskMap = new HashMap<>();

        taskExecutorMapWrapper.forEach(taskExecutor -> {
          final Pair<Long, Long> input = taskExecutor.getTaskMetrics().getInputReceiveProcessElement();


          if (prevCountMap.containsKey(taskExecutor.getId())) {
            final Pair<Long, Long> prevInput = prevCountMap.get(taskExecutor.getId());
            final long elapsed = System.currentTimeMillis() - prevFlush;
            final long receiveRate = input.left() - prevInput.left();
            final long processRate = input.right() - processingCntHistory.get(taskExecutor.getId()).get(0);
            final long queueLength = input.left() - input.right();

            final double timeToProcessRemain;
            if (processRate == 0) {
              timeToProcessRemain = 0;
            } else {
              timeToProcessRemain = queueLength / (double) processRate;
            }

            if (timeToProcessRemain > 2) {
              // if (processRate > 0 && receiveRate > 0 && processRate < 0.9 * receiveRate) {
              // send throttle signal
              final double reduceRatio = 1 - (processRate / (double) receiveRate);
              // final int throttleDelay = (int)(reduceRatio * delay);

              final int throttleDelay = (int) (delay);

              taskExecutor.getTask().getUpstreamTasks().entrySet().forEach(entry -> {
                final List<String> dstTasks = entry.getValue();

                dstTasks.forEach(dstTask -> {
                  if (!throttleDstTaskMaxDelayMap.containsKey(dstTask)
                    || throttleDstTaskMaxDelayMap.get(dstTask) < throttleDelay) {
                    throttleDstTaskMaxDelayMap.put(dstTask, throttleDelay);
                    throttleDstTaskMap.put(dstTask, Triple.of(dstTask, entry.getKey().getId(), taskExecutor.getId()));
                  }
                });
              });
            }
          }


          prevCountMap.put(taskExecutor.getId(), input);
          final List<Long> l = new LinkedList<>();
          processingCntHistory.putIfAbsent(taskExecutor.getId(), l);
          processingCntHistory.get(taskExecutor.getId()).add(input.right());

          if (l.size() == 11) {
            l.remove(0);
          }
        });

        throttleDstTaskMap.entrySet().forEach(entry -> {
          final String dstTask = entry.getKey();
          final Triple<String, String, String> val = entry.getValue();
          final int throttleDelay = throttleDstTaskMaxDelayMap.get(dstTask);

          LOG.info("Send throttle from {}/{}", throttleDelay, val.getRight());

          pipeManagerWorker.sendSignalForInputPipes(Collections.singletonList(dstTask),
            val.getMiddle(), val.getRight(),
            (triple) -> {
              return new TaskControlMessage(
                TaskControlMessage.TaskControlMessageType
                  .THROTTLE,
                triple.getLeft(), // my output pipe index
                triple.getMiddle(), // my input pipe index
                triple.getRight(),  // srct ask id
                throttleDelay);
            });
        });
        */
      } catch (final Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

      }, period, period, TimeUnit.MILLISECONDS);
    }


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
    this.stageExecutorThreadMap = stageExecutorThreadMap;
  }

  public void start() {
    taskScheduledMapWorker.init();
    executorChannelManagerMap.init();

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

  private void onTaskReceived(final Task task,
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

  public void activate() {
    activated = true;
  }

  public void deactivate() {
    activated = false;
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
      synchronized (this) {
        final ExecutorThread executorThread;
        if (onLambda) {
          // find min alloc executor
          final int numTask = numReceivedTasks.getAndIncrement();
          final OptionalInt minOccupancy =
            executorThreads.getExecutorThreads().stream()
              .map(et -> et.getNumTasks())
              .mapToInt(i -> i).min();

          executorThread = executorThreads.getExecutorThreads()
            .stream()
            .filter(et -> et.getNumTasks() == minOccupancy.getAsInt())
            .findFirst()
            .orElseThrow(() -> new RuntimeException("No such executor"));
        } else {
          final Set<String> o2oStages = task.getO2oStages();

          // locality-aware scheduling in executor
          final Optional<ExecutorThread> o2oThread = o2oStages.stream()
            .map(o2oStageId -> {
              final String srcTaskId = RuntimeIdManager.generateTaskId(o2oStageId,
                RuntimeIdManager.getIndexFromTaskId(task.getTaskId()), 0);
              return srcTaskId;
            })
            .filter(o2oTaskId -> taskExecutorMapWrapper.containsTask(o2oTaskId))
            .map(o2oScheduleTaskId -> taskExecutorMapWrapper.getTaskExecutorThread(o2oScheduleTaskId))
            .findFirst();

          if (o2oThread.isPresent()) {
            executorThread = o2oThread.get();
          } else {
            // find min alloc executor
            final int numTask = numReceivedTasks.getAndIncrement();
            final OptionalInt minOccupancy =
              executorThreads.getExecutorThreads().stream()
                .map(et -> et.getNumTasks())
                .mapToInt(i -> i).min();

            executorThread = executorThreads.getExecutorThreads()
              .stream()
              .filter(et -> et.getNumTasks() == minOccupancy.getAsInt())
              .findFirst()
              .orElseThrow(() -> new RuntimeException("No such executor"));
          }
        }

        TaskExecutor taskExecutor;

        if (task.isStreamTask()) {
          //  taskExecutor = new StreamTaskExecutorImpl(
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
          // if (evalConf.optimizationPolicy.contains("R3")) {
          if (evalConf.optimizationPolicy.contains("R1R3")) {
            taskExecutor =
              new R1R3CRTaskExecutorImpl(
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
            if (task.getTaskOutgoingEdges().stream()
              .anyMatch(edge -> !(edge.getDataCommunicationPattern()
                .equals(CommunicationPatternProperty.Value.OneToOne) ||
                edge.getDataCommunicationPattern()
                  .equals(CommunicationPatternProperty.Value.TransientOneToOne)))) {
              taskExecutor =
                new R3CRTaskExecutorImpl(
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
                new SingleO2OOutputR3CRTaskExecutorImpl(
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
          }
        } else if (task.isMerger()) {
          LOG.info("optimization policy {}", evalConf.optimizationPolicy);
          if (evalConf.optimizationPolicy.contains("R1R3")) {
            taskExecutor =
              new R1R3MergerTaskExecutorImpl(
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
              new MergerTaskExecutorImpl(
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
        } else {
          if (evalConf.optimizationPolicy.contains("R3")
            && task.isParitalCombine()) {
            taskExecutor =
              new PartialTaskExecutorImpl(
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
            LOG.info("DefaultTaskExecutor for {}", task.getTaskId());
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
        }

        LOG.info("Add Task {} to {} thread of {}, time {}", taskExecutor.getId(), executorThread, executorId,
          System.currentTimeMillis() - st);

        executorThread.addNewTask(taskExecutor);

        LOG.info("Put Task time {} to {} thread of {}, time {}", taskExecutor.getId(), executorThread, executorId,
          System.currentTimeMillis() - st);
      }
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

    private final ScheduledExecutorService throttler = Executors.newSingleThreadScheduledExecutor();
    private final AlarmManager alarmManager = new AlarmManager();

    @Override
    public synchronized void onMessage(final ControlMessage.Message message) {
      switch (message.getType()) {
        case CancelAndSourceSleep: {
          LOG.info("Cancel Throttle prev sleep in {}", message.getSetNum(), executorId);
          alarmManager.deactivateAlarms();
        }
        case SourceSleep: {
          LOG.info("Throttle source {} sleep in {}", message.getSetNum(), executorId);
          /*
          final long sleepDelay = message.getSetNum();
          final long window = Util.THROTTLE_WINDOW;

          executorThreads.getExecutorThreads().forEach(executorThread -> {
            final BackpressureSleepAlarm alarm =
              new BackpressureSleepAlarm(executorThread, throttler, alarmManager, sleepDelay, window);
            alarm.triggerNextSleep();
          });
          */

          executorThreads.getExecutorThreads().forEach(executorThread -> {
            executorThread.backpressure();
            /*
            executorThread.addShortcutEvent(new TaskControlMessage(
              TaskControlMessage.TaskControlMessageType.SOURCE_SLEEP,
              -1, -1, "", message.getSetNum()));
              */
          });
          break;
        }
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

            // For R3 reshaping
            // Checkpoint if task is not partial combine
            // Otherwise, we should not checkpoint the partial combine task
            // because state merger will merge its state
            final ExecutorThread executorThread = taskExecutorMapWrapper.getTaskExecutorThread(message.getStopTaskMsg().getTaskId());

            if (task.isParitalCombine()) {
              executorThread.addShortcutEvent(new TaskControlMessage(
                TaskControlMessage.TaskControlMessageType.R3_INVOKE_REDIRECTION_FOR_CR_BY_MASTER, -1, -1,
                message.getStopTaskMsg().getTaskId(), false));
            } else {
              executorThread.addShortcutEvent(new TaskControlMessage(
                TaskControlMessage.TaskControlMessageType.R2_INVOKE_REDIRECTION_FOR_CR_BY_MASTER, -1, -1,
                message.getStopTaskMsg().getTaskId(), true));
            }
          });
          break;
        }
        // Deactivate lambda in init of executor
        case DeactivateLambdaTask: {
          // for R2 reshaping
          LOG.info("Deactivation lambda task in executor {}", executorId);
          taskExecutorMapWrapper.getTaskExecutorMap()
            .keySet().forEach(taskExecutor -> {
              if (taskExecutor.getTask().isTransientTask()) {
                executorService.execute(() -> {
                  final ExecutorThread executorThread = taskExecutorMapWrapper.getTaskExecutorThread(taskExecutor.getId());
                  executorThread.addEvent(new TaskControlMessage(
                    TaskControlMessage.TaskControlMessageType.R2_INVOKE_REDIRECTION_FOR_CR_BY_MASTER, -1, -1,
                    taskExecutor.getId(), false));
                /*
                if (taskExecutor.getTask().isParitalCombine()) {
                   executorThread.addEvent(new TaskControlMessage(
                    TaskControlMessage.TaskControlMessageType.R3_INIT, -1, -1,
                    taskExecutor.getId(), false));
                } else {
                  executorThread.addEvent(new TaskControlMessage(
                    TaskControlMessage.TaskControlMessageType.R2_INVOKE_REDIRECTION_FOR_CR_BY_MASTER, -1, -1,
                    taskExecutor.getId(), false));
                }
                */
                });
              }
            });
          break;
        }
        case R2Init: {
          // for R2 reshaping
          final String taskId = message.getStopTaskMsg().getTaskId();
          final Task task = taskExecutorMapWrapper.getTaskExecutor(taskId).getTask();
          executorService.execute(() -> {
            LOG.info("Get state signal from VM task {} to Lambda {} in executor {}",
              task.getPairTaskId(), message.getStopTaskMsg().getTaskId(), executorId);

            final ExecutorThread executorThread = taskExecutorMapWrapper.getTaskExecutorThread(message.getStopTaskMsg().getTaskId());
            executorThread.addShortcutEvent(new TaskControlMessage(
              TaskControlMessage.TaskControlMessageType.R2_INIT, -1, -1,
              message.getStopTaskMsg().getTaskId(), null));
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
              TaskControlMessage.TaskControlMessageType.R2_GET_STATE_SIGNAL_BY_PAIR, -1, -1,
              message.getStopTaskMsg().getTaskId(), null));
          });

          break;
        }
        case R3PairInputOutputStart: {
          // for R2 reshaping
          final String taskId = message.getStopTaskMsg().getTaskId();
          final Task task = taskExecutorMapWrapper.getTaskExecutor(taskId).getTask();
          executorService.execute(() -> {
            LOG.info("Get state signal from VM task {} to Lambda {} in executor {}",
              task.getPairTaskId(), message.getStopTaskMsg().getTaskId(), executorId);

            final ExecutorThread executorThread = taskExecutorMapWrapper.getTaskExecutorThread(message.getStopTaskMsg().getTaskId());
            executorThread.addShortcutEvent(new TaskControlMessage(
              TaskControlMessage.TaskControlMessageType.R3_INPUT_OUTPUT_START_BY_PAIR, -1, -1,
              message.getStopTaskMsg().getTaskId(), null));
          });
          break;
        }
        case R3OptSignalFinalCombine: {
          // for R2 reshaping
          final String taskId = message.getStopTaskMsg().getTaskId();
          final Task task = taskExecutorMapWrapper.getTaskExecutor(taskId).getTask();
          executorService.execute(() -> {
            LOG.info("Get opt fial signal from VM task {} to Lambda {} in executor {}",
              task.getPairTaskId(), message.getStopTaskMsg().getTaskId(), executorId);
            final ExecutorThread executorThread = taskExecutorMapWrapper.getTaskExecutorThread(message.getStopTaskMsg().getTaskId());
            executorThread.addShortcutEvent(new TaskControlMessage(
              TaskControlMessage.TaskControlMessageType.R3_OPT_SIGNAL_FINAL_COMBINE_BY_PAIR, -1, -1,
              message.getStopTaskMsg().getTaskId(), null));
          });
          break;
        }
        case R3AckPairTaskInitiateProtocol: {
          final String taskId = message.getStopTaskMsg().getTaskId();
          final Task task = taskExecutorMapWrapper.getTaskExecutor(taskId).getTask();
          executorService.execute(() -> {
            LOG.info("Get R3AckPairTaskInitiateProtocol {}",
              message.getStopTaskMsg().getTaskId(), executorId);

            final ExecutorThread executorThread = taskExecutorMapWrapper.getTaskExecutorThread(message.getStopTaskMsg().getTaskId());
            executorThread.addShortcutEvent(new TaskControlMessage(
              TaskControlMessage.TaskControlMessageType.R3_ACK_PAIR_TASK_INITIATE_REROUTING_PROTOCOL, -1, -1,
              message.getStopTaskMsg().getTaskId(), null));
          });
          break;
        }
        case R3PairTaskInitiateProtocol: {
          final String taskId = message.getStopTaskMsg().getTaskId();
          final Task task = taskExecutorMapWrapper.getTaskExecutor(taskId).getTask();
          executorService.execute(() -> {
            LOG.info("Get R3PairTaskInitiateProtocol {}",
              message.getStopTaskMsg().getTaskId(), executorId);

            final ExecutorThread executorThread = taskExecutorMapWrapper.getTaskExecutorThread(message.getStopTaskMsg().getTaskId());
            executorThread.addShortcutEvent(new TaskControlMessage(
              TaskControlMessage.TaskControlMessageType.R3_PAIR_TASK_INITIATE_REROUTING_PROTOCOL, -1, -1,
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
              TaskControlMessage.TaskControlMessageType.R2_TASK_OUTPUT_START_BY_PAIR, -1, -1,
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
        case ScheduleCachedTask:
          executorService.execute(() -> {
            final long st = System.currentTimeMillis();
            final Task task = prevScheduledTaskCacheMap.get(message.getStopTaskMsg().getTaskId());

            // executorMetrics.taskInputReceiveRateMap
            //  .put(task.getTaskId(), Pair.of(new AtomicLong(), new AtomicLong()));

            LOG.info("Task {} received in executor {}, serialized time {}", task.getTaskId(), executorId, System.currentTimeMillis() - st);
            onTaskReceived(task, null);
          });
          break;
        case ScheduleTask:
          executorService.execute(() -> {
            final long st = System.currentTimeMillis();
            final ControlMessage.ScheduleTaskMsg scheduleTaskMsg = message.getScheduleTaskMsg();
            final byte[] bytes = scheduleTaskMsg.getTask().toByteArray();
            final ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            final DataInputStream dis = new DataInputStream(bis);
            final Task task = Task.decode(dis);
            prevScheduledTaskCacheMap.put(task.getTaskId(), task);

            // executorMetrics.taskInputReceiveRateMap
            //  .put(task.getTaskId(), Pair.of(new AtomicLong(), new AtomicLong()));

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
