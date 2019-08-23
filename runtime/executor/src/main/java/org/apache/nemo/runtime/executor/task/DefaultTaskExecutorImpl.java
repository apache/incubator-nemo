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
package org.apache.nemo.runtime.executor.task;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.Edge;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.ir.edge.executionproperty.AdditionalOutputTagProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.ir.vertex.transform.MessageAggregatorTransform;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.punctuation.EmptyElement;
import org.apache.nemo.common.punctuation.Finishmark;
import org.apache.nemo.common.punctuation.TimestampAndValue;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.offloading.common.ServerlessExecutorProvider;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.TaskLocationMap;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.common.state.TaskState;
import org.apache.nemo.runtime.executor.*;
import org.apache.nemo.runtime.executor.bytetransfer.ByteTransport;
import org.apache.nemo.runtime.executor.common.*;
import org.apache.nemo.runtime.executor.common.datatransfer.DataFetcherOutputCollector;
import org.apache.nemo.runtime.executor.common.datatransfer.InputReader;
import org.apache.nemo.runtime.executor.common.datatransfer.IteratorWithNumBytes;
import org.apache.nemo.runtime.executor.data.BroadcastManagerWorker;
import org.apache.nemo.runtime.executor.data.PipeManagerWorker;
import org.apache.nemo.runtime.executor.data.SerializerManager;
import org.apache.nemo.runtime.executor.datatransfer.*;
import org.apache.nemo.runtime.executor.relayserver.RelayServer;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingResultEvent;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingResultTimestampEvent;
import org.apache.nemo.runtime.lambdaexecutor.StateOutput;
import org.apache.nemo.runtime.lambdaexecutor.Triple;
import org.apache.nemo.runtime.lambdaexecutor.datatransfer.RendevousServerClient;
import org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOffloadingOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Executes a task.
 * Should be accessed by a single thread.
 */
@NotThreadSafe
public final class DefaultTaskExecutorImpl implements TaskExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultTaskExecutorImpl.class.getName());

  // Essential information
  private final Task task;
  private final String taskId;
  private final List<SourceVertexDataFetcher> sourceVertexDataFetchers;
  private final BroadcastManagerWorker broadcastManagerWorker;
  private final MetricMessageSender metricMessageSender;

  private final PersistentConnectionToMasterMap persistentConnectionToMasterMap;

  private final SerializerManager serializerManager;

  private final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag;

  // Variables for offloading - start
  private final ServerlessExecutorProvider serverlessExecutorProvider;
  private final Map<String, Pair<OperatorMetricCollector, OutputCollector>> vertexIdAndCollectorMap;
  private final Set<OutputWriter> outputWriterMap;
  private final Map<String, List<String>> taskOutgoingEdges;
  private final Map<String, NextIntraTaskOperatorInfo> operatorInfoMap = new HashMap<>();
  private final EvalConf evalConf;
  // Variables for offloading - end

  private final long adjustTime;

  private byte[] serializedDag;

  private transient OffloadingContext currOffloadingContext = null;

  //private final ConcurrentLinkedQueue<ControlEvent> controlEventQueue = new ConcurrentLinkedQueue<>();

  private final Map<Long, Integer> watermarkCounterMap = new HashMap<>();
  private final Map<Long, Long> prevWatermarkMap = new HashMap<>();

  // key: offloading sink, val:
  //                            - left: watermarks emitted from the offloading header
  //                            - right: pending watermarks
  public final Map<String, Pair<PriorityQueue<Watermark>, PriorityQueue<Watermark>>> expectedWatermarkMap = new HashMap<>();


  final Map<String, Double> samplingMap = new HashMap<>();


  private final BlockingQueue<OffloadingRequestEvent> offloadingRequestQueue = new LinkedBlockingQueue<>();

  private final int pollingInterval = 20; // ms
  private final ScheduledExecutorService pollingTrigger;

  private boolean pollingTime = false;
  private boolean kafkaOffloading = false;
  //private final OffloadingWorkerFactory offloadingWorkerFactory;

  private final AtomicInteger processedCnt = new AtomicInteger(0);
  private final AtomicLong prevOffloadStartTime = new AtomicLong(System.currentTimeMillis());
  private final AtomicLong prevOffloadEndTime = new AtomicLong(System.currentTimeMillis());

  private boolean isStateless = true;

  private final AtomicReference<Status> status = new AtomicReference<>(Status.RUNNING);
  private final ByteTransport byteTransport;
  private final String executorId;
  private final PipeManagerWorker pipeManagerWorker;
  private final PersistentConnectionToMasterMap toMaster;

  private final long threadId;

  private final List<DataFetcher> allFetchers = new ArrayList<>();
  final Optional<Offloader> offloader;

  private EventHandler<Integer> offloadingDoneHandler;
  private EventHandler<Integer> endOffloadingHandler;

  private final TaskInputContextMap taskInputContextMap;

  private final AtomicLong taskExecutionTime = new AtomicLong(0);

  private long offloadedExecutionTime = 0;
  private final TinyTaskOffloadingWorkerManager tinyWorkerManager;

  private final List<StageEdge> copyOutgoingEdges;
  private final List<StageEdge> copyIncomingEdges;

  private final RelayServer relayServer;
  private final TaskLocationMap taskLocationMap;

  private final ExecutorService prepareService;

  private final ExecutorGlobalInstances executorGlobalInstances;

  private final RendevousServerClient rendevousServerClient;

  private final ExecutorThread executorThread;

  private final AtomicBoolean prepared = new AtomicBoolean(false);

  /**
   * Constructor.
   *
   * @param task                   Task with information needed during execution.
   * @param irVertexDag            A DAG of vertices.
   * @param taskStateManager       State manager for this Task.
   * @param intermediateDataIOFactory    For reading from/writing to data to other tasks.
   * @param broadcastManagerWorker For broadcasts.
   * @param metricMessageSender    For sending metric with execution stats to the master.
   * @param persistentConnectionToMasterMap For sending messages to the master.
   */
  public DefaultTaskExecutorImpl(final long threadId,
                                 final String executorId,
                                 final ByteTransport byteTransport,
                                 final PersistentConnectionToMasterMap toMaster,
                                 final PipeManagerWorker pipeManagerWorker,
                                 final Task task,
                                 final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
                                 final List<StageEdge> copyOutgoingEdges,
                                 final List<StageEdge> copyIncomingEdges,
                                 final TaskStateManager taskStateManager,
                                 final IntermediateDataIOFactory intermediateDataIOFactory,
                                 final BroadcastManagerWorker broadcastManagerWorker,
                                 final MetricMessageSender metricMessageSender,
                                 final PersistentConnectionToMasterMap persistentConnectionToMasterMap,
                                 final SerializerManager serializerManager,
                                 final ServerlessExecutorProvider serverlessExecutorProvider,
                                 final TinyTaskOffloadingWorkerManager tinyWorkerManager,
                                 final EvalConf evalConf,
                                 final TaskInputContextMap taskInputContextMap,
                                 final RelayServer relayServer,
                                 final TaskLocationMap taskLocationMap,
                                 final ExecutorService prepareService,
                                 final ExecutorGlobalInstances executorGlobalInstances,
                                 final RendevousServerClient rendevousServerClient,
                                 final ExecutorThread executorThread) {
    // Essential information
    //LOG.info("Non-copied outgoing edges: {}", task.getTaskOutgoingEdges());
    this.copyOutgoingEdges = copyOutgoingEdges;
    this.executorThread = executorThread;
    //LOG.info("Copied outgoing edges: {}, bytes: {}", copyOutgoingEdges);
    this.copyIncomingEdges = copyIncomingEdges;
    this.prepareService = prepareService;
    this.executorGlobalInstances = executorGlobalInstances;

    this.rendevousServerClient = rendevousServerClient;

    this.relayServer = relayServer;
    this.taskLocationMap = taskLocationMap;

    this.pollingTrigger = executorGlobalInstances.getPollingTrigger();

    this.threadId = threadId;
    this.executorId = executorId;
    this.byteTransport = byteTransport;
    this.toMaster = toMaster;
    this.pipeManagerWorker = pipeManagerWorker;
    this.sourceVertexDataFetchers = new ArrayList<>();
    this.task = task;
    this.irVertexDag = irVertexDag;
    this.taskId = task.getTaskId();
    this.broadcastManagerWorker = broadcastManagerWorker;
    this.tinyWorkerManager = tinyWorkerManager;
    this.vertexIdAndCollectorMap = new HashMap<>();
    this.outputWriterMap = new HashSet<>();
    this.taskOutgoingEdges = new HashMap<>();
    this.taskInputContextMap = taskInputContextMap;
    task.getTaskOutgoingEdges().forEach(edge -> {
      final IRVertex src = edge.getSrcIRVertex();
      final IRVertex dst = edge.getDstIRVertex();
      taskOutgoingEdges.putIfAbsent(src.getId(), new LinkedList<>());
      taskOutgoingEdges.get(src.getId()).add(dst.getId());
    });

    samplingMap.putAll(evalConf.samplingJson);

    this.evalConf = evalConf;

    this.serverlessExecutorProvider = serverlessExecutorProvider;

    this.serializerManager = serializerManager;

    // Metric sender
    this.metricMessageSender = metricMessageSender;

    // Dynamic optimization
    // Assigning null is very bad, but we are keeping this for now

    this.persistentConnectionToMasterMap = persistentConnectionToMasterMap;

    // Prepare data structures
    prepare(task, irVertexDag, intermediateDataIOFactory);

    prepared.set(true);

    LOG.info("Source vertex data fetchers in defaultTaskExecutorimpl: {}", sourceVertexDataFetchers);

    this.offloader = getOffloader();

    LOG.info("Executor address map: {}", byteTransport.getExecutorAddressMap());

    /*
    pollingTrigger.scheduleAtFixedRate(() -> {
      pollingTime = true;
    }, pollingInterval, pollingInterval, TimeUnit.MILLISECONDS);
    */

    if (evalConf.isLocalSource) {
      this.adjustTime = System.currentTimeMillis() - 1436918400000L;
    } else {
      this.adjustTime = 0;
    }

    // For latency logging
    for (final Pair<OperatorMetricCollector, OutputCollector> metricCollector :
      vertexIdAndCollectorMap.values()) {
      metricCollector.left().setAdjustTime(adjustTime);
    }

    /*
    if (evalConf.enableOffloading || evalConf.offloadingdebug) {
      offloadingService.execute(() -> {
        try {
          handleOffloadingRequestEvent();
        } catch (InterruptedException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      });
    }
    */
  }

  @Override
  public boolean isSourceAvailable() {
    //LOG.info("Source available in {}", taskId);
    for (final SourceVertexDataFetcher sourceVertexDataFetcher : sourceVertexDataFetchers) {
      if (sourceVertexDataFetcher.isAvailable()) {
        return true;
      }
    }

    return false;
  }

  @Override
  public ExecutorThread getExecutorThread() {
    return executorThread;
  }

  @Override
  public boolean isSource() {
    return sourceVertexDataFetchers.size() > 0;
  }

  @Override
  public PendingState getPendingStatus() {
    if (offloader.isPresent()) {
      return offloader.get().getPendingStatus();
    }

    throw new RuntimeException("not supported");
  }

  @Override
  public boolean isFinished() {
    return false;
  }

  @Override
  public void finish() {
    // do nothing
  }

  @Override
  public void setOffloadedTaskTime(long t) {
    offloadedExecutionTime = t;
  }

  @Override
  public AtomicLong getTaskExecutionTime() {
    return taskExecutionTime;
  }

  @Override
  public OutputCollector getVertexOutputCollector(final String vertexId) {
    return vertexIdAndCollectorMap.get(vertexId).right();
  }

  public long calculateOffloadedTaskTime() {
    return offloadedExecutionTime;
    /*
    long sum = 0L;
    for (final Long val : offloadedTaskTimeMap.values()) {
      sum += (val / 1000);
    }
    //return offloadedTaskTimeMap.values().stream().reduce(0L, (x,y) -> x/1000+y/1000);
    return sum;
    */
  }

  private Optional<Offloader> getOffloader() {

    final Optional<Offloader> offloader;

    if (evalConf.enableOffloading || evalConf.offloadingdebug) {

      offloader = Optional.of(new TinyTaskOffloader(
          executorId,
          this,
          evalConf,
          serializedDag,
          copyOutgoingEdges,
          copyIncomingEdges,
          tinyWorkerManager,
          taskOutgoingEdges,
          sourceVertexDataFetchers,
          taskId,
          sourceVertexDataFetchers.size() > 0 ? sourceVertexDataFetchers.get(0) : null,
          status,
          prevOffloadStartTime,
          prevOffloadEndTime,
          toMaster,
          outputWriterMap,
          irVertexDag,
        taskLocationMap,
        executorThread,
        allFetchers));

    } else {
      offloader = Optional.empty();
    }

    return offloader;
  }

  @Override
  public long getThreadId() {
    return threadId;
  }
  @Override
  public boolean isRunning() {
    return status.get() == Status.RUNNING;
  }
  @Override
  public boolean isOffloadPending() {
    return status.get() == Status.OFFLOAD_PENDING;
  }
  @Override
  public boolean isOffloaded() {
    return status.get() == Status.OFFLOADED;
  }
  @Override
  public boolean isDeoffloadPending() {
    return status.get() == Status.DEOFFLOAD_PENDING;
  }
  @Override
  public String getId() {
    return taskId;
  }

  @Override
  public boolean isStateless() {
    return isStateless;
  }
  @Override
  public AtomicInteger getProcessedCnt() {
    return processedCnt;
  }
  @Override
  public AtomicLong getPrevOffloadStartTime() {
    return prevOffloadStartTime;
  }
  @Override
  public AtomicLong getPrevOffloadEndTime() {
    return prevOffloadEndTime;
  }

  @Override
  public void startOffloading(final long baseTime,
                              final Object worker,
                              final EventHandler<Integer> doneHandler) {
    offloadingDoneHandler = doneHandler;
    executorThread.queue.add(() -> {
      if (offloader != null && offloader.isPresent()) {
        LOG.info("Start -- handle start offloading kafka event {}", taskId);
        offloader.get().handleStartOffloadingEvent((TinyTaskWorker) worker);
        LOG.info("End -- handle start offloading kafka event {}", taskId);
      }
    });
    //offloadingRequestQueue.add(new OffloadingRequestEvent(true, baseTime,
    //  (TinyTaskWorker) worker));
  }

  @Override
  public void endOffloading(final EventHandler<Integer> handler) {
    endOffloadingHandler = handler;
    executorThread.queue.add(() -> {
      if (offloader.isPresent()) {
        LOG.info("Start -- Receive end offloading event {}", taskId);
        offloader.get().handleEndOffloadingEvent();
        LOG.info("End -- Receive end offloading event {}", taskId);
      }

    });
  }


  /**
   * Converts the DAG of vertices into pointer-based DAG of vertex harnesses.
   * This conversion is necessary for constructing concrete data channels for each vertex's inputs and outputs.
   * <p>
   * - Source vertex read: Explicitly handled (SourceVertexDataFetcher)
   * - Sink vertex write: Implicitly handled within the vertex
   * <p>
   * - Parent-task read: Explicitly handled (ParentTaskDataFetcher)
   * - Children-task write: Explicitly handled (VertexHarness)
   * <p>
   * - Intra-task read: Implicitly handled when performing Intra-task writes
   * - Intra-task write: Explicitly handled (VertexHarness)
   * <p>
   * For element-wise data processing, we traverse vertex harnesses from the roots to the leaves for each element.
   * This means that overheads associated with jumping from one harness to the other should be minimal.
   * For example, we should never perform an expensive hash operation to traverse the harnesses.
   *
   * @param task        task.
   * @param irVertexDag dag.
   * @param intermediateDataIOFactory intermediate IO.
   * @return fetchers and harnesses.
   */
  private List<VertexHarness> prepare(
    final Task task,
    final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
    final IntermediateDataIOFactory intermediateDataIOFactory) {
    final int taskIndex = RuntimeIdManager.getIndexFromTaskId(task.getTaskId());

    // Traverse in a reverse-topological order to ensure that each visited vertex's children vertices exist.
    final List<IRVertex> reverseTopologicallySorted = Lists.reverse(irVertexDag.getTopologicalSort());

    // Build a map for edge as a key and edge index as a value
    // This variable is used for creating NextIntraTaskOperatorInfo
    // in {@link this#getInternalMainOutputs and this#internalMainOutputs}
    final Map<Edge, Integer> edgeIndexMap = new HashMap<>();
    reverseTopologicallySorted.forEach(childVertex -> {

      if (childVertex.isStateful) {
        isStateless = false;
      }

      // FOR OFFLOADING
      expectedWatermarkMap.put(childVertex.getId(), Pair.of(new PriorityQueue<>(), new PriorityQueue<>()));

      if (irVertexDag.getOutgoingEdgesOf(childVertex.getId()).size() == 0) {
        childVertex.isSink = true;
        // If it is sink or emit to next stage, we log the latency
        LOG.info("MonitoringVertex: {}", childVertex.getId());
        if (!samplingMap.containsKey(childVertex.getId())) {
          samplingMap.put(childVertex.getId(), 1.0);
        }
        LOG.info("Sink vertex: {}", childVertex.getId());
      }

      final List<Edge> edges = TaskExecutorUtil.getAllIncomingEdges(task, irVertexDag, childVertex);
      for (int edgeIndex = 0; edgeIndex < edges.size(); edgeIndex++) {
        final Edge edge = edges.get(edgeIndex);
        edgeIndexMap.putIfAbsent(edge, edgeIndex);
      }
    });


    serializedDag = SerializationUtils.serialize(irVertexDag);

    // Build a map for InputWatermarkManager for each operator vertex
    // This variable is used for creating NextIntraTaskOperatorInfo
    // in {@link this#getInternalMainOutputs and this#internalMainOutputs}
    final Map<IRVertex, InputWatermarkManager> operatorWatermarkManagerMap = new HashMap<>();
    reverseTopologicallySorted.forEach(childVertex -> {

      if (childVertex instanceof OperatorVertex) {
        final List<Edge> edges = TaskExecutorUtil.getAllIncomingEdges(task, irVertexDag, childVertex);

        if (edges.size() == 1) {
          operatorWatermarkManagerMap.putIfAbsent(childVertex,
            new SingleInputWatermarkManager(
              new OperatorWatermarkCollector((OperatorVertex) childVertex),
              childVertex,
              expectedWatermarkMap,
              prevWatermarkMap,
              watermarkCounterMap));
        } else {
          operatorWatermarkManagerMap.putIfAbsent(childVertex,
            new MultiInputWatermarkManager(childVertex, edges.size(),
              new OperatorWatermarkCollector((OperatorVertex) childVertex)));
        }
      }
    });

    // Create a harness for each vertex
    final Map<String, VertexHarness> vertexIdToHarness = new HashMap<>();

    reverseTopologicallySorted.forEach(irVertex -> {
      final Optional<Readable> sourceReader = TaskExecutorUtil.getSourceVertexReader(irVertex, task.getIrVertexIdToReadable());
      if (sourceReader.isPresent() != irVertex instanceof SourceVertex) {
        throw new IllegalStateException(irVertex.toString());
      }

      // Additional outputs
      final Map<String, List<NextIntraTaskOperatorInfo>> internalAdditionalOutputMap =
        getInternalOutputMap(irVertex, irVertexDag, edgeIndexMap, operatorWatermarkManagerMap);

      final Map<String, List<OutputWriter>> externalAdditionalOutputMap =
        TaskExecutorUtil.getExternalAdditionalOutputMap(
          irVertex, task.getTaskOutgoingEdges(), intermediateDataIOFactory, taskId, outputWriterMap,
          expectedWatermarkMap, prevWatermarkMap, watermarkCounterMap, rendevousServerClient);

      for (final List<NextIntraTaskOperatorInfo> interOps : internalAdditionalOutputMap.values()) {
        for (final NextIntraTaskOperatorInfo interOp : interOps) {
          operatorInfoMap.put(interOp.getNextOperator().getId(), interOp);
        }
      }

      // Main outputs
      final List<NextIntraTaskOperatorInfo> internalMainOutputs;
      if (internalAdditionalOutputMap.containsKey(AdditionalOutputTagProperty.getMainOutputTag())) {
        internalMainOutputs = internalAdditionalOutputMap.remove(AdditionalOutputTagProperty.getMainOutputTag());
      } else {
        internalMainOutputs = new ArrayList<>();
      }

      final List<OutputWriter> externalMainOutputs =
        TaskExecutorUtil.getExternalMainOutputs(
          irVertex, task.getTaskOutgoingEdges(), intermediateDataIOFactory, taskId, outputWriterMap,
          expectedWatermarkMap, prevWatermarkMap, watermarkCounterMap, rendevousServerClient);

      OutputCollector outputCollector;

      if (irVertex instanceof OperatorVertex
        && ((OperatorVertex) irVertex).getTransform() instanceof MessageAggregatorTransform) {
        outputCollector = new RunTimeMessageOutputCollector(
          taskId, irVertex, persistentConnectionToMasterMap, this);
      } else {

        final List<RuntimeEdge<IRVertex>> edges = irVertexDag.getOutgoingEdgesOf(irVertex);
        final List<IRVertex> dstVertices = irVertexDag.getOutgoingEdgesOf(irVertex).
          stream().map(edge -> edge.getDst()).collect(Collectors.toList());

        OperatorMetricCollector omc;


        if (!dstVertices.isEmpty()) {
          omc = new OperatorMetricCollector(irVertex,
            dstVertices,
            serializerManager.getSerializer(edges.get(0).getId()),
            edges.get(0),
            evalConf,
            watermarkCounterMap,
            samplingMap,
            taskId);


          outputCollector = new OperatorVertexOutputCollector(
            vertexIdAndCollectorMap,
            irVertex, internalMainOutputs, internalAdditionalOutputMap,
            externalMainOutputs, externalAdditionalOutputMap, omc,
            prevWatermarkMap, expectedWatermarkMap, this,
            edges.get(0).getId(), taskId, samplingMap);

        } else {
          omc = new OperatorMetricCollector(irVertex,
            dstVertices,
            null,
            null,
            evalConf,
            watermarkCounterMap,
            samplingMap,
            taskId);

          outputCollector = new OperatorVertexOutputCollector(
            vertexIdAndCollectorMap,
            irVertex, internalMainOutputs, internalAdditionalOutputMap,
            externalMainOutputs, externalAdditionalOutputMap, omc,
            prevWatermarkMap, expectedWatermarkMap, this, null,
            taskId, samplingMap);
        }

        vertexIdAndCollectorMap.put(irVertex.getId(), Pair.of(omc, outputCollector));
      }

      // Create VERTEX HARNESS
      final VertexHarness vertexHarness = new VertexHarness(
        irVertex, outputCollector, new TransformContextImpl(
          broadcastManagerWorker, irVertex, serverlessExecutorProvider, taskId),
        externalMainOutputs, externalAdditionalOutputMap);

      TaskExecutorUtil.prepareTransform(vertexHarness);
      vertexIdToHarness.put(irVertex.getId(), vertexHarness);

      // Prepare data READ
      // Source read
      // TODO[SLS]: should consider multiple outgoing edges
      // All edges will have the same encoder/decoder!
      if (irVertex instanceof SourceVertex) {
        final RuntimeEdge edge = irVertexDag.getOutgoingEdgesOf(irVertex).get(0);
        LOG.info("SourceVertex: {}, edge: {}", irVertex.getId(), edge.getId());

        // Source vertex read
        final SourceVertexDataFetcher fe = new SourceVertexDataFetcher(
          (SourceVertex) irVertex,
          edge,
          sourceReader.get(),
          outputCollector,
          prepareService,
          taskId,
          executorGlobalInstances);

        sourceVertexDataFetchers.add(fe);
        allFetchers.add(fe);
      }

      // Parent-task read
      // TODO #285: Cache broadcasted data
      task.getTaskIncomingEdges()
        .stream()
        .filter(inEdge -> inEdge.getDstIRVertex().getId().equals(irVertex.getId())) // edge to this vertex
        .map(incomingEdge -> {

          LOG.info("Incoming edge: {}, taskIndex: {}, taskId: {}", incomingEdge, taskIndex, taskId);

          return Pair.of(incomingEdge, intermediateDataIOFactory
            .createReader(taskIndex, incomingEdge.getSrcIRVertex(), incomingEdge, this));
        })
        .forEach(pair -> {
          if (irVertex instanceof OperatorVertex) {

            final StageEdge edge = pair.left();
            final int edgeIndex = edgeIndexMap.get(edge);
            final InputWatermarkManager watermarkManager = operatorWatermarkManagerMap.get(irVertex);
            final InputReader parentTaskReader = pair.right();
            final OutputCollector dataFetcherOutputCollector =
              new DataFetcherOutputCollector(edge.getSrcIRVertex(), (OperatorVertex) irVertex,
                outputCollector, edgeIndex, watermarkManager);


            //final OperatorMetricCollector omc = new OperatorMetricCollector(edge.getSrcIRVertex(),
            //  Arrays.asList(edge.getDstIRVertex()),
            //  serializerManager.getSerializer(edge.getId()), edge, evalConf, shutdownExecutor);

            //metricCollectors.add(Pair.of(omc, outputCollector));

            if (parentTaskReader instanceof PipeInputReader) {
              isStateless = false;
              allFetchers.add(
                new MultiThreadParentTaskDataFetcher(
                  taskId,
                  parentTaskReader.getSrcIrVertex(),
                  edge,
                  parentTaskReader,
                  dataFetcherOutputCollector,
                  rendevousServerClient,
                  executorGlobalInstances,
                  this,
                  prepared));
            } else {
              allFetchers.add(
                new ParentTaskDataFetcher(
                  parentTaskReader.getSrcIrVertex(),
                  edge,
                  parentTaskReader,
                  dataFetcherOutputCollector));
            }
          }
        });
    });


    final List<VertexHarness> sortedHarnessList = irVertexDag.getTopologicalSort()
      .stream()
      .map(vertex -> vertexIdToHarness.get(vertex.getId()))
      .collect(Collectors.toList());



    return sortedHarnessList;
  }

  /**
   * Process a data element down the DAG dependency.
   */
  private void processElement(final OutputCollector outputCollector, final TimestampAndValue dataElement) {
    processedCnt.getAndIncrement();
    outputCollector.setInputTimestamp(dataElement.timestamp);
    outputCollector.emit(dataElement.value);
  }

  private void processWatermark(final OutputCollector outputCollector,
                                final Watermark watermark) {
    outputCollector.emitWatermark(watermark);
  }

  /**
   * Execute a task, while handling unrecoverable errors and exceptions.
   */
  @Override
  public void execute() {
    throw new RuntimeException("Not supported");
  }

  private void finalizeVertex(final VertexHarness vertexHarness) {
    closeTransform(vertexHarness);
    finalizeOutputWriters(vertexHarness);
  }
  @Override
  public void sendToServerless(final Object event,
                               final List<String> nextOperatorIds,
                               final long wm,
                               final String edgeId) {
    offloader.get().offloadingData(event, nextOperatorIds, wm, edgeId);
  }


  // exeutor thread가 바로 부르는 method
  @Override
  public boolean handleSourceData() {
    boolean processed = false;

    for (final SourceVertexDataFetcher dataFetcher : sourceVertexDataFetchers) {
      final Object event = dataFetcher.fetchDataElement();
      if (!event.equals(EmptyElement.getInstance()))  {
        onEventFromDataFetcher(event, dataFetcher);
        processed = true;
      }
    }
    return processed;
  }

  /**
   * Process an event generated from the dataFetcher.
   * If the event is an instance of Finishmark, we remove the dataFetcher from the current list.
   * @param event event
   * @param dataFetcher current data fetcher
   */
  private void onEventFromDataFetcher(final Object event,
                                      final DataFetcher dataFetcher) {

    if (event instanceof Finishmark) {
      // We've consumed all the data from this data fetcher.
    } else if (event instanceof Watermark) {
      // Watermark
      processWatermark(dataFetcher.getOutputCollector(), (Watermark) event);
    } else if (event instanceof TimestampAndValue) {

      // This is for latency logging
      /*
      if (isFirstEvent) {
        final long elapsedTime = System.currentTimeMillis() - taskStartTime;
        isFirstEvent = false;
        adjustTime = taskStartTime - ((TimestampAndValue) event).timestamp;
        for (final Pair<OperatorMetricCollector, OutputCollector> metricCollector :
          vertexIdAndCollectorMap.values()) {
          metricCollector.left().setAdjustTime(adjustTime);
        }
      }
      */

      // Process data element
      processElement(dataFetcher.getOutputCollector(), (TimestampAndValue) event);
    } else {
      throw new RuntimeException("Invalid type of event: " + event);
    }
  }

  // For offloading!
  private void handleOffloadingEvent(final Triple<List<String>, String, Object> triple) {
    //LOG.info("Result handle {} / {} / {}", triple.first, triple.second, triple.third);

    final Object elem = triple.third;

    for (final String nextOpId : triple.first) {
      if (operatorInfoMap.containsKey(nextOpId)) {
        final NextIntraTaskOperatorInfo interOp = operatorInfoMap.get(nextOpId);
        final OutputCollector collector = vertexIdAndCollectorMap.get(nextOpId).right();

        //LOG.info("Emit data to {}, {}, {}, {}", nextOpId, interOp, collector, elem);

        if (elem instanceof Watermark) {
          final Watermark watermark = (Watermark) elem;
          LOG.info("Receive watermark {} for {}", watermark, interOp.getNextOperator().getId());
          interOp.getWatermarkManager().trackAndEmitWatermarks(interOp.getEdgeIndex(), watermark);

        } else if (elem instanceof TimestampAndValue) {
          final TimestampAndValue tsv = (TimestampAndValue) elem;
          //LOG.info("Receive data {}", tsv);
          final long currTime = System.currentTimeMillis();
          final long latency = currTime - tsv.timestamp;
          LOG.info("Event Latency {} from lambda {} in {}", latency, nextOpId, taskId);

          // do not process these data!!
          //collector.setInputTimestamp(tsv.timestamp);
          //interOp.getNextOperator().getTransform().onData(tsv.value);
        } else {
          throw new RuntimeException("Unknown type: " + elem);
        }
      } else {
        throw new RuntimeException("Unknown type: " + elem);

        /*
        // this is for output writer
        final OutputWriter outputWriter = outputWriterMap.get(nextOpId);
        if (elem instanceof Watermark) {
          outputWriter.writeWatermark((Watermark) elem);
        } else if (elem instanceof TimestampAndValue) {
          outputWriter.write(elem);
        } else {
          throw new RuntimeException("Unknown type: " + elem);
        }
        */
      }
    }
  }


  @Override
  public void handleIntermediateWatermarkEvent(final Object element, final DataFetcher dataFetcher) {

    executorThread.decoderThread.execute(() -> {
      executorThread.queue.add(() -> {
        //LOG.info("handler watermark");
        onEventFromDataFetcher(element, dataFetcher);
      });
    });
  }

  @Override
  public void handleIntermediateData(IteratorWithNumBytes iterator, DataFetcher dataFetcher) {
    if (iterator.hasNext()) {
      executorThread.decoderThread.execute(() -> {
        while (iterator.hasNext()) {
          final Object element = iterator.next();
          if (prepared.get()) {
            executorThread.queue.add(() -> {
              if (!element.equals(EmptyElement.getInstance())) {
                //LOG.info("handle intermediate data {}, {}", element, dataFetcher);
                onEventFromDataFetcher(element, dataFetcher);
              }
            });
          }
        }
      });
    }
  }

  @Override
  public void handleOffloadingEvent(final Object data) {
    executorThread.queue.add(() -> {
      offloadingEventHandler(data);
    });
  }

  private void offloadingEventHandler(final Object data) {
    if (data instanceof OffloadingResultEvent) {
      final OffloadingResultEvent msg = (OffloadingResultEvent) data;
      LOG.info("Result processed in executor: cnt {}, watermark: {}", msg.data.size(), msg.watermark);

      for (final Triple<List<String>, String, Object> triple : msg.data) {
        //LOG.info("Data {}, {}, {}", triple.first, triple.second, triple.third);
        handleOffloadingEvent(triple);
      }
    }  else if (data instanceof OffloadingResultTimestampEvent) {
      final OffloadingResultTimestampEvent event = (OffloadingResultTimestampEvent) data;
      final long currTime = System.currentTimeMillis();
      final long latency = currTime - event.timestamp;
      LOG.info("Event Latency {} from lambda {} in {}, ts: {}", latency, event.vertexId, taskId, event.timestamp);

    } else if (data instanceof KafkaOffloadingOutput) {

      if (offloader.isPresent()) {
        offloader.get().handleOffloadingOutput((KafkaOffloadingOutput) data);
      }
      endOffloadingHandler.onNext(1);

    } else if (data instanceof StateOutput) {

      if (offloader.isPresent()) {
        offloader.get().handleStateOutput((StateOutput) data);
      }
      endOffloadingHandler.onNext(1);

    } else if (data instanceof OffloadingDoneEvent) {
      final OffloadingDoneEvent e = (OffloadingDoneEvent) data;
      LOG.info("Offloading done of {}", e.taskId);
      offloadingDoneHandler.onNext(1);

    } else {
      throw new RuntimeException("Unsupported type: " + data);
    }
  }

  /**
   * Return a map of Internal Outputs associated with their output tag.
   * If an edge has no output tag, its info are added to the mainOutputTag.
   *
   * @param irVertex source irVertex
   * @param irVertexDag DAG of IRVertex and RuntimeEdge
   * @param edgeIndexMap Map of edge and index
   * @param operatorWatermarkManagerMap Map of irVertex and InputWatermarkManager
   * @return Map<OutputTag, List<NextIntraTaskOperatorInfo>>
   */
  private Map<String, List<NextIntraTaskOperatorInfo>> getInternalOutputMap(
    final IRVertex irVertex,
    final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
    final Map<Edge, Integer> edgeIndexMap,
    final Map<IRVertex, InputWatermarkManager> operatorWatermarkManagerMap) {
    // Add all intra-task tags to additional output map.
    final Map<String, List<NextIntraTaskOperatorInfo>> map = new HashMap<>();

    irVertexDag.getOutgoingEdgesOf(irVertex.getId())
      .stream()
      .map(edge -> {
          final boolean isPresent = edge.getPropertyValue(AdditionalOutputTagProperty.class).isPresent();
          final String outputTag;
          if (isPresent) {
            outputTag = edge.getPropertyValue(AdditionalOutputTagProperty.class).get();
          } else {
            outputTag = AdditionalOutputTagProperty.getMainOutputTag();
          }
          final int index = edgeIndexMap.get(edge);
          final OperatorVertex nextOperator = (OperatorVertex) edge.getDst();
          final InputWatermarkManager inputWatermarkManager = operatorWatermarkManagerMap.get(nextOperator);
          return Pair.of(outputTag, new NextIntraTaskOperatorInfo(index, edge, nextOperator, inputWatermarkManager));
        })
      .forEach(pair -> {
        map.putIfAbsent(pair.left(), new ArrayList<>());
        map.get(pair.left()).add(pair.right());
      });

    return map;
  }

  ////////////////////////////////////////////// Transform-specific helper methods

  private void closeTransform(final VertexHarness vertexHarness) {
    final IRVertex irVertex = vertexHarness.getIRVertex();
    final Transform transform;
    if (irVertex instanceof OperatorVertex) {
      transform = ((OperatorVertex) irVertex).getTransform();
      transform.close();
    }

    vertexHarness.getContext().getSerializedData().ifPresent(data ->
      persistentConnectionToMasterMap.getMessageSender(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID).send(
        ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID)
          .setType(ControlMessage.MessageType.ExecutorDataCollected)
          .setDataCollected(ControlMessage.DataCollectMessage.newBuilder().setData(data).build())
          .build()));
  }

  ////////////////////////////////////////////// Misc

  public void setIRVertexPutOnHold(final IRVertex irVertex) {
  }

  /**
   * Finalize the output write of this vertex.
   * As element-wise output write is done and the block is in memory,
   * flush the block into the designated data store and commit it.
   *
   * @param vertexHarness harness.
   */
  private void finalizeOutputWriters(final VertexHarness vertexHarness) {
    final List<Long> writtenBytesList = new ArrayList<>();

    // finalize OutputWriters for main children
    vertexHarness.getWritersToMainChildrenTasks().forEach(outputWriter -> {
      outputWriter.close();
      final Optional<Long> writtenBytes = outputWriter.getWrittenBytes();
      writtenBytes.ifPresent(writtenBytesList::add);
    });

    // finalize OutputWriters for additional tagged children
    vertexHarness.getWritersToAdditionalChildrenTasks().values().forEach(outputWriters -> {
      outputWriters.forEach(outputWriter -> {
        outputWriter.close();
        final Optional<Long> writtenBytes = outputWriter.getWrittenBytes();
        writtenBytes.ifPresent(writtenBytesList::add);
      });
    });

    long totalWrittenBytes = 0;
    for (final Long writtenBytes : writtenBytesList) {
      totalWrittenBytes += writtenBytes;
    }

    // TODO #236: Decouple metric collection and sending logic
    metricMessageSender.send("TaskMetric", taskId,
      "writtenBytes", SerializationUtils.serialize(totalWrittenBytes));
  }


  @Override
  public void close() throws Exception {

  }

  @Override
  public String toString() {
    return taskId;
  }
}
