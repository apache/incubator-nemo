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
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.TaskMetrics;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.Edge;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.ir.edge.executionproperty.AdditionalOutputTagProperty;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.transform.MessageAggregatorTransform;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.punctuation.EmptyElement;
import org.apache.nemo.common.punctuation.Finishmark;
import org.apache.nemo.common.punctuation.TimestampAndValue;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.compiler.frontend.beam.source.UnboundedSourceReadable;
import org.apache.nemo.compiler.frontend.beam.transform.GBKFinalTransform;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.offloading.common.ServerlessExecutorProvider;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.common.Task;
import org.apache.nemo.runtime.executor.*;
import org.apache.nemo.runtime.executor.common.*;
import org.apache.nemo.runtime.executor.common.datatransfer.*;
import org.apache.nemo.common.StateStore;
import org.apache.nemo.runtime.executor.data.BroadcastManagerWorker;
import org.apache.nemo.runtime.executor.data.SerializerManager;
import org.apache.nemo.runtime.executor.datatransfer.*;
import org.apache.nemo.runtime.lambdaexecutor.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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

  final Map<String, Double> samplingMap = new HashMap<>();

  private final AtomicInteger processedCnt = new AtomicInteger(0);

  private boolean isStateless = true;

  private final String executorId;

  private final long threadId;

  private final List<DataFetcher> allFetchers = new ArrayList<>();
  public final Optional<Offloader> offloader;

  private final AtomicLong taskExecutionTime = new AtomicLong(0);

  private final ExecutorService prepareService;

  private final ExecutorThreadQueue executorThreadQueue;

  private final AtomicBoolean prepared = new AtomicBoolean(false);

  private Transform statefulTransform;

  private final TaskMetrics taskMetrics;

  private final StateStore stateStore;

  private final TaskInputWatermarkManager taskWatermarkManager;
  private final InputPipeRegister inputPipeRegister;

  /**
   * Constructor.
   *
   * @param task                   Task with information needed during execution.
   * @param irVertexDag            A DAG of vertices.
   * @param intermediateDataIOFactory    For reading from/writing to data to other tasks.
   * @param broadcastManagerWorker For broadcasts.
   * @param metricMessageSender    For sending metric with execution stats to the master.
   * @param persistentConnectionToMasterMap For sending messages to the master.
   */
  public DefaultTaskExecutorImpl(final long threadId,
                                 final String executorId,
                                 final Task task,
                                 final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
                                 final IntermediateDataIOFactory intermediateDataIOFactory,
                                 final BroadcastManagerWorker broadcastManagerWorker,
                                 final MetricMessageSender metricMessageSender,
                                 final PersistentConnectionToMasterMap persistentConnectionToMasterMap,
                                 final SerializerManager serializerManager,
                                 final ServerlessExecutorProvider serverlessExecutorProvider,
                                 final EvalConf evalConf,
                                 final ExecutorService prepareService,
                                 final ExecutorThreadQueue executorThreadQueue,
                                 final InputPipeRegister inputPipeRegister,
                                 final StateStore stateStore) {
    // Essential information
    //LOG.info("Non-copied outgoing edges: {}", task.getTaskOutgoingEdges());
    this.stateStore = stateStore;
    this.taskMetrics = new TaskMetrics();
    this.executorThreadQueue = executorThreadQueue;
    //LOG.info("Copied outgoing edges: {}, bytes: {}", copyOutgoingEdges);
    this.prepareService = prepareService;
    this.inputPipeRegister = inputPipeRegister;
    this.taskWatermarkManager = new TaskInputWatermarkManager();
    this.threadId = threadId;
    this.executorId = executorId;
    this.sourceVertexDataFetchers = new ArrayList<>();
    this.task = task;
    this.irVertexDag = irVertexDag;
    this.taskId = task.getTaskId();
    this.broadcastManagerWorker = broadcastManagerWorker;
    this.vertexIdAndCollectorMap = new HashMap<>();
    this.outputWriterMap = new HashSet<>();
    this.taskOutgoingEdges = new HashMap<>();

    task.getTaskOutgoingEdges().forEach(edge -> {
      LOG.info("Task outgoing edge {}", edge);
      final IRVertex src = edge.getSrcIRVertex();
      final IRVertex dst = edge.getDstIRVertex();
      taskOutgoingEdges.putIfAbsent(src.getId(), new LinkedList<>());
      taskOutgoingEdges.get(src.getId()).add(dst.getId());
      final Integer taskIndex = RuntimeIdManager.getIndexFromTaskId(task.getTaskId());

      // bidrectional !!
      final int parallelism = edge
        .getSrcIRVertex().getPropertyValue(ParallelismProperty.class).get();

      final CommunicationPatternProperty.Value comm =
        edge.getPropertyValue(CommunicationPatternProperty.class).get();

      if (comm.equals(CommunicationPatternProperty.Value.OneToOne)) {
        inputPipeRegister.registerInputPipe(
          RuntimeIdManager.generateTaskId(edge.getDst().getId(), taskIndex, 0),
          edge.getId(),
          task.getTaskId(),
          new PipeInputReader(edge.getDstIRVertex(), taskId, (RuntimeEdge) edge,
          serializerManager.getSerializer(((RuntimeEdge)edge).getId()), executorThreadQueue));
      } else {
        for (int i = 0; i < parallelism; i++) {
          inputPipeRegister.registerInputPipe(
            RuntimeIdManager.generateTaskId(edge.getDst().getId(), i, 0),
            edge.getId(),
            task.getTaskId(),
            new PipeInputReader(edge.getDstIRVertex(), taskId, (RuntimeEdge) edge,
          serializerManager.getSerializer(((RuntimeEdge)edge).getId()), executorThreadQueue));
        }
      }
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

    // TODO: Initialize states for the task
    // TODO: restart output writers and sources if it is moved

    // Prepare data structures
    prepare(task, irVertexDag, intermediateDataIOFactory);
    prepared.set(true);

    LOG.info("Source vertex data fetchers in defaultTaskExecutorimpl: {}", sourceVertexDataFetchers);

    this.offloader = Optional.empty();
    // getOffloader();

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
  public boolean hasData() {
    for (final SourceVertexDataFetcher sourceVertexDataFetcher : sourceVertexDataFetchers) {
      if (sourceVertexDataFetcher.hasData()) {
        return true;
      }
    }

    return false;
  }

  @Override
  public Task getTask() {
    return task;
  }

  public TaskMetrics getTaskMetrics() {
    return taskMetrics;
  }

  @Override
  public int getNumKeys() {
    if (isStateless) {
      return 0;
    } else {
      //LOG.info("Key {}, {}", num, taskId);
      return statefulTransform.getNumKeys();
    }
  }

  @Override
  public boolean isSource() {
    return sourceVertexDataFetchers.size() > 0;
  }

  @Override
  public AtomicLong getTaskExecutionTime() {
    return taskExecutionTime;
  }

  @Override
  public long getThreadId() {
    return threadId;
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
  private void prepare(
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
        if (childVertex instanceof OperatorVertex) {
          final OperatorVertex ov = (OperatorVertex) childVertex;
          statefulTransform = ov.getTransform();
          LOG.info("Set GBK final transform");
        }
      }

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


    // serializedDag = SerializationUtils.serialize(irVertexDag);

    // Create a harness for each vertex
    final Map<String, VertexHarness> vertexIdToHarness = new HashMap<>();

    reverseTopologicallySorted.forEach(irVertex -> {
      final Optional<Readable> sourceReader = getSourceVertexReader(irVertex, task.getIrVertexIdToReadable());
      if (sourceReader.isPresent() != irVertex instanceof SourceVertex) {
        throw new IllegalStateException(irVertex.toString());
      }

      // Additional outputs
      final Map<String, List<NextIntraTaskOperatorInfo>> internalAdditionalOutputMap =
        getInternalOutputMap(irVertex, irVertexDag, edgeIndexMap);

      final Map<String, List<OutputWriter>> externalAdditionalOutputMap =
        TaskExecutorUtil.getExternalAdditionalOutputMap(
          irVertex, task.getTaskOutgoingEdges(), intermediateDataIOFactory, taskId, outputWriterMap,
          taskMetrics);

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
          taskMetrics);

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
            samplingMap,
            taskId);

          outputCollector = new OperatorVertexOutputCollector(
            vertexIdAndCollectorMap,
            irVertex, internalMainOutputs, internalAdditionalOutputMap,
            externalMainOutputs, externalAdditionalOutputMap, omc,
            taskId, samplingMap);

        } else {
          omc = new OperatorMetricCollector(irVertex,
            dstVertices,
            null,
            null,
            samplingMap,
            taskId);

          outputCollector = new OperatorVertexOutputCollector(
            vertexIdAndCollectorMap,
            irVertex, internalMainOutputs, internalAdditionalOutputMap,
            externalMainOutputs, externalAdditionalOutputMap, omc,
            taskId, samplingMap);
        }

        vertexIdAndCollectorMap.put(irVertex.getId(), Pair.of(omc, outputCollector));
      }

      // Create VERTEX HARNESS
      final VertexHarness vertexHarness = new VertexHarness(
        irVertex, outputCollector, new TransformContextImpl(
          broadcastManagerWorker, irVertex, serverlessExecutorProvider, taskId, stateStore),
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
          prepared,
          new Readable.ReadableContext() {
            @Override
            public StateStore getStateStore() {
              return stateStore;
            }
            @Override
            public String getTaskId() {
              return taskId;
            }
          });

        sourceVertexDataFetchers.add(fe);
        allFetchers.add(fe);

        if (sourceVertexDataFetchers.size() > 1) {
          throw new RuntimeException("Source vertex data fetcher is larger than one");
        }
      }

      // Parent-task read
      // TODO #285: Cache broadcasted data
      task.getTaskIncomingEdges()
        .stream()
        .filter(inEdge -> inEdge.getDstIRVertex().getId().equals(irVertex.getId())) // edge to this vertex
        .map(incomingEdge -> {

          LOG.info("Incoming edge: {}, taskIndex: {}, taskId: {}", incomingEdge, taskIndex, taskId);

          return Pair.of(incomingEdge, intermediateDataIOFactory
            .createReader(
              taskIndex,
              taskId,
              incomingEdge.getSrcIRVertex(), incomingEdge, executorThreadQueue));
        })
        .forEach(pair -> {
          if (irVertex instanceof OperatorVertex) {

            final StageEdge edge = pair.left();
            final int edgeIndex = edgeIndexMap.get(edge);
            final InputReader parentTaskReader = pair.right();
            final OutputCollector dataFetcherOutputCollector =
              new DataFetcherOutputCollector(edge.getSrcIRVertex(), (OperatorVertex) irVertex,
                outputCollector, edgeIndex, taskId);

            if (parentTaskReader instanceof PipeInputReader) {
              final int parallelism = edge
                .getSrcIRVertex().getPropertyValue(ParallelismProperty.class).get();

              final CommunicationPatternProperty.Value comm =
                edge.getPropertyValue(CommunicationPatternProperty.class).get();

              final DataFetcher df = new MultiThreadParentTaskDataFetcher(
                taskId,
                edge.getSrcIRVertex(),
                edge,
                parentTaskReader,
                dataFetcherOutputCollector);

              if (comm.equals(CommunicationPatternProperty.Value.OneToOne)) {
                inputPipeRegister.registerInputPipe(
                  RuntimeIdManager.generateTaskId(edge.getSrc().getId(), taskIndex, 0),
                  edge.getId(),
                  task.getTaskId(),
                  parentTaskReader);

                taskWatermarkManager.addDataFetcher(df, 1);

              } else {
                for (int i = 0; i < parallelism; i++) {
                  inputPipeRegister.registerInputPipe(
                    RuntimeIdManager.generateTaskId(edge.getSrc().getId(), i, 0),
                    edge.getId(),
                    task.getTaskId(),
                    parentTaskReader);
                }

                taskWatermarkManager.addDataFetcher(df, parallelism);
              }

              isStateless = false;
              allFetchers.add(df);

            } else {
              allFetchers.add(
                new ParentTaskDataFetcher(
                  edge.getSrcIRVertex(),
                  edge,
                  parentTaskReader,
                  dataFetcherOutputCollector));
            }
          }
        });
    });
    // return sortedHarnessList;
  }

  /**
   * Process a data element down the DAG dependency.
   */
  private void processElement(final OutputCollector outputCollector, final TimestampAndValue dataElement) {
    final long ns = System.nanoTime();

    processedCnt.getAndIncrement();
    outputCollector.setInputTimestamp(dataElement.timestamp);
    outputCollector.emit(dataElement.value);

    final long endNs = System.nanoTime();

    taskMetrics.incrementComputation(endNs - ns);
  }

  private void processWatermark(final OutputCollector outputCollector,
                                final Watermark watermark) {
    outputCollector.emitWatermark(watermark);
  }

  @Override
  public void handleData(final DataFetcher dataFetcher, Object event) {
    if (event instanceof Finishmark) {
      // We've consumed all the data from this data fetcher.
    } else if (event instanceof WatermarkWithIndex) {
      // Watermark
      // LOG.info("Handling watermark with index {}", event);
      final WatermarkWithIndex d = (WatermarkWithIndex) event;
      final Optional<Watermark> watermark =
        taskWatermarkManager.updateWatermark(dataFetcher, d.getIndex(), d.getWatermark().getTimestamp());

      if (watermark.isPresent()) {
        // LOG.info("Emitting watermark for {} / {}", taskId, new Instant(watermark.get().getTimestamp()));
        processWatermark(dataFetcher.getOutputCollector(), watermark.get());
      }
    } else if (event instanceof Watermark) {
      // This MUST BE generated from input source
      if (!isSource()) {
        throw new RuntimeException("Invalid watermark !! this task is not source " + taskId);
      }
      processWatermark(sourceVertexDataFetchers.get(0).getOutputCollector(), (Watermark) event);
    } else if (event instanceof TimestampAndValue) {
      // This MUST BE generated from remote source
      taskMetrics.incrementInputElement();
      // Process data element
      processElement(dataFetcher.getOutputCollector(), (TimestampAndValue) event);
    } else {
      throw new RuntimeException("Invalid type of event: " + event);
    }
  }

  // exeutor thread가 바로 부르는 method
  @Override
  public boolean handleSourceData() {
    boolean processed = false;

    for (final SourceVertexDataFetcher dataFetcher : sourceVertexDataFetchers) {
      final Object event = dataFetcher.fetchDataElement();
      if (!event.equals(EmptyElement.getInstance()))  {
        handleData(dataFetcher, event);
        processed = true;
        //executorMetrics.increaseInputCounter(stageId);
      }
    }
    return processed;
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
          // interOp.getWatermarkManager().trackAndEmitWatermarks(interOp.getEdgeIndex(), watermark);

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

  public boolean offloaded = false;


  /**
   * Return a map of Internal Outputs associated with their output tag.
   * If an edge has no output tag, its info are added to the mainOutputTag.
   *
   * @param irVertex source irVertex
   * @param irVertexDag DAG of IRVertex and RuntimeEdge
   * @param edgeIndexMap Map of edge and index
   * @return Map<OutputTag, List<NextIntraTaskOperatorInfo>>
   */
  private Map<String, List<NextIntraTaskOperatorInfo>> getInternalOutputMap(
    final IRVertex irVertex,
    final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
    final Map<Edge, Integer> edgeIndexMap) {
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
          return Pair.of(outputTag, new NextIntraTaskOperatorInfo(index, edge, nextOperator));
        })
      .forEach(pair -> {
        map.putIfAbsent(pair.left(), new ArrayList<>());
        map.get(pair.left()).add(pair.right());
      });

    return map;
  }

  ////////////////////////////////////////////// Transform-specific helper methods



  public Optional<Readable> getSourceVertexReader(final IRVertex irVertex,
                                                  final Map<String, Readable> irVertexIdToReadable) {
    if (irVertex instanceof SourceVertex) {
      final Readable readable = irVertexIdToReadable.get(irVertex.getId());
      return Optional.of(readable);
    } else {
      return Optional.empty();
    }
  }

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

  private boolean finished = false;

  @Override
  public boolean checkpoint() {

    boolean hasChekpoint = false;
    for (final DataFetcher dataFetcher : allFetchers) {
      if (dataFetcher instanceof SourceVertexDataFetcher) {
        if (hasChekpoint) {
          throw new RuntimeException("Double checkpoint..." + taskId);
        }

        hasChekpoint = true;
        final SourceVertexDataFetcher srcDataFetcher = (SourceVertexDataFetcher) dataFetcher;
        final Readable readable = srcDataFetcher.getReadable();
        readable.checkpoint();
      }

      try {
        dataFetcher.close();
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

    if (!isStateless) {
      statefulTransform.checkpoint();
    }

    return true;
  }


  @Override
  public String toString() {
    return taskId;
  }
}
