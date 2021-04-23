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
package org.apache.nemo.runtime.executor.common.tasks;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.Task;
import org.apache.nemo.common.TaskMetrics;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.punctuation.Finishmark;
import org.apache.nemo.common.punctuation.TimestampAndValue;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.common.punctuation.WatermarkWithIndex;
import org.apache.nemo.offloading.common.ServerlessExecutorProvider;
import org.apache.nemo.offloading.common.StateStore;
import org.apache.nemo.offloading.common.TaskHandlingEvent;
import org.apache.nemo.runtime.executor.common.*;
import org.apache.nemo.runtime.executor.common.datatransfer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.DataInputStream;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.nemo.runtime.executor.common.TaskExecutorUtil.getDstTaskIds;

@NotThreadSafe
public final class PartialTaskExecutorImpl implements TaskExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(PartialTaskExecutorImpl.class.getName());

  // Essential information
  private final Task task;
  private final String taskId;
  private final List<SourceVertexDataFetcher> sourceVertexDataFetchers;
  private final SerializerManager serializerManager;

  private final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag;

  // Variables for prepareOffloading - start
  private final ServerlessExecutorProvider serverlessExecutorProvider;
  private final Map<String, Pair<OperatorMetricCollector, OutputCollector>> vertexIdAndCollectorMap;
  private final Map<String, List<String>> taskOutgoingEdges;
  private final Map<String, NextIntraTaskOperatorInfo> operatorInfoMap = new HashMap<>();
  // Variables for prepareOffloading - end

  private final long adjustTime;

  final Map<String, Double> samplingMap;

  private boolean isStateless = true;

  private final String executorId;

  private final long threadId;

  private final List<DataFetcher> allFetchers = new ArrayList<>();

  private final ExecutorService prepareService;

  private final ExecutorThreadQueue executorThreadQueue;

  private final AtomicBoolean prepared = new AtomicBoolean(false);

  private List<Transform> statefulTransforms;

  private final TaskMetrics taskMetrics;

  private final StateStore stateStore;

  private WatermarkTracker taskWatermarkManager;
  private final InputPipeRegister inputPipeRegister;

  // private final OffloadingManager offloadingManager;

  private final PipeManagerWorker pipeManagerWorker;

  private final Map<String, DataFetcher> edgeToDataFetcherMap = new HashMap<>();

  private final OutputCollectorGenerator outputCollectorGenerator;

  private final boolean offloaded;

  final Map<String, List<OutputWriter>> externalAdditionalOutputMap;

  private final IntermediateDataIOFactory intermediateDataIOFactory;

  private final int taskIndex;

  private final Transform pToFinalTransform;
  private final OutputCollector finalOutputCollector;

  private final EncoderFactory shortcutEncoder;
  private final Serializer shortcutSerializer;
  private final String mergerEdgeId;
  private final String mergerTaskId;

  private boolean pairTaskStopped = true;

  private final Map<String, Object> sharedObject;

  private final OutputCollector partialOutputCollector;
  private final Serializer serializer;
  private PartialOutputEmitter partialOutputEmitter;

  private final PartialOutputEmitter pToRemoteEmitter;
  private final PartialOutputEmitter pToLocalCombiner;

  /**
   * 무조건 single o2o (normal) - o2o (transient) 를 input으로 받음.
   * Output: single o2o (normal) - o2o (transient)
   */
  public PartialTaskExecutorImpl(final long threadId,
                                 final String executorId,
                                 final Task task,
                                 final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
                                 final IntermediateDataIOFactory intermediateDataIOFactory,
                                 final SerializerManager serializerManager,
                                 final ServerlessExecutorProvider serverlessExecutorProvider,
                                 final Map<String, Double> samplingMap,
                                 final boolean isLocalSource,
                                 final ExecutorService prepareService,
                                 final ExecutorThreadQueue executorThreadQueue,
                                 final InputPipeRegister inputPipeRegister,
                                 final StateStore stateStore,
                                 //  final OffloadingManager offloadingManager,
                                 final PipeManagerWorker pipeManagerWorker,
                                 final OutputCollectorGenerator outputCollectorGenerator,
                                 final byte[] bytes,
                                 final Transform.ConditionalRouting conditionalRouting,
                                 // final OffloadingPreparer offloadingPreparer,
                                 final boolean offloaded) {
    // Essential information
    //LOG.info("Non-copied outgoing edges: {}", task.getTaskOutgoingEdges());
    this.offloaded = offloaded;
    this.intermediateDataIOFactory = intermediateDataIOFactory;
    this.outputCollectorGenerator = outputCollectorGenerator;
    this.pipeManagerWorker = pipeManagerWorker;
    this.sharedObject = new HashMap<>();
    // this.offloadingManager = offloadingManager;
    this.stateStore = stateStore;
    this.taskMetrics = new TaskMetrics();
    this.executorThreadQueue = executorThreadQueue;
    //LOG.info("Copied outgoing edges: {}, bytes: {}", copyOutgoingEdges);
    this.prepareService = prepareService;
    this.inputPipeRegister = inputPipeRegister;
    this.taskId = task.getTaskId();
    this.taskIndex = RuntimeIdManager.getIndexFromTaskId(taskId);

    this.externalAdditionalOutputMap = new HashMap<>();

    this.statefulTransforms = new ArrayList<>();

    final long restoresSt = System.currentTimeMillis();


    LOG.info("Task {} watermark manager restore time {}", taskId, System.currentTimeMillis() - restoresSt);

    this.threadId = threadId;
    this.executorId = executorId;
    this.sourceVertexDataFetchers = new ArrayList<>();
    this.task = task;
    this.irVertexDag = irVertexDag;
    this.vertexIdAndCollectorMap = new HashMap<>();
    this.taskOutgoingEdges = new HashMap<>();
    this.samplingMap = samplingMap;
    // samplingMap.putAll(evalConf.samplingJson);

    this.serverlessExecutorProvider = serverlessExecutorProvider;

    this.serializerManager = serializerManager;

    final RuntimeEdge mergerEdge = task.getTaskOutgoingEdges().stream()
      .findFirst().get();

    this.mergerEdgeId = mergerEdge.getId();
    this.mergerTaskId = getDstTaskIds(taskId, mergerEdge).get(0);

    this.taskWatermarkManager = restoreTaskInputWatermarkManager().orElse(getTaskWatermarkManager());

    if (isLocalSource) {
      this.adjustTime = System.currentTimeMillis() - 1436918400000L;
    } else {
      this.adjustTime = 0;
    }

    final OperatorVertex gbkVertex = (OperatorVertex)irVertexDag.getVertices().stream()
      .filter(vertex -> vertex.isStateful).findFirst().get();

    this.pToFinalTransform = gbkVertex.getPartialToFinalTransform();

    if (pToFinalTransform == null ){
      throw new RuntimeException("Partial task " + taskId + " has null final transform " +
        gbkVertex.getId());
    }

    this.shortcutEncoder = new NemoEventEncoderFactory(gbkVertex.getOriginEncoderFactory());
    this.shortcutSerializer = new Serializer(shortcutEncoder, null, null, null);

    this.finalOutputCollector = new OutputCollector() {
      private long ts;
      @Override
      public void setInputTimestamp(long timestamp) {
        ts = timestamp;
      }

      @Override
      public long getInputTimestamp() {
        return ts;
      }

      @Override
      public void emit(Object output) {
//        if (taskId.contains("Stage17") || taskId.contains("Stage18")) {
//        LOG.info("Final output to remote for {}: {}", taskId, output);
//      }
//        if (taskId.contains("Stage6")) {
//          LOG.info("Partial output from local final for {}: ts: {} {}", taskId,
//            new Instant(ts));
//        }
        final long timestamp = ts;
        // LOG.info("Emit pTofinal output in {}, element: {}", taskId, output);
        writeData(new TimestampAndValue<>(timestamp, output));
      }

      @Override
      public void emitWatermark(Watermark watermark) {
//        if (taskId.contains("Stage6")) {
//          LOG.info("Partial watermark from local final for {}: ts: {} {}", taskId,
//            new Instant(watermark.getTimestamp()));
//        }
        // LOG.info("SM vertex {} emits watermark {}", taskId, watermark.getTimestamp());
        writeData(new WatermarkWithIndex(watermark, taskIndex));
      }

      @Override
      public void emit(String dstVertexId, Object output) {
        throw new RuntimeException("Not support additional output in partial task " + taskId + ", " + dstVertexId);
      }
    };

    this.serializer = serializerManager.getSerializer(mergerEdgeId);
    this.pToLocalCombiner = new PartialOutputEmitToLocalFinal();
    this.pToRemoteEmitter = new PartialOutputEmitToRemoteMerger();

    // TODO: preserve this value when migratrion
    if (task.isTransientTask()) {
      setPairTaskStopped(false);
    } else {
      setPairTaskStopped(true);
    }


    this.partialOutputCollector = new OutputCollector() {
      private long ts;
      @Override
      public void setInputTimestamp(long timestamp) {
        ts = timestamp;
      }

      @Override
      public long getInputTimestamp() {
        return ts;
      }

      @Override
      public void emit(Object output) {
        partialOutputEmitter.emit(output, ts);
      }

      @Override
      public void emitWatermark(Watermark watermark) {
        partialOutputEmitter.emitWatermark(watermark);
      }

      @Override
      public void emit(String dstVertexId, Object output) {
        throw new RuntimeException("Not support additional output in partial task " + taskId + ", " + dstVertexId);
      }
    };

    LOG.info("Preparing {}", taskId);
    pToFinalTransform.prepare(new TransformContextImpl(
      null, null, taskId, stateStore,
        null, executorId,
        sharedObject),
      finalOutputCollector);

    prepare();
  }

  private void writeData(Object data) {
    pipeManagerWorker.writeData(taskId, mergerEdgeId, mergerTaskId, shortcutSerializer, data);
  }

  @Override
  public boolean isSourceAvailable() {
    throw new RuntimeException("not supported");
  }


  @Override
  public void setThrottleSourceRate(final long num) {
  }

  @Override
  public DefaultTaskExecutorImpl.CurrentState getStatus() {
    return null;
  }

  // For source task
  @Override
  public boolean hasData() {
    throw new RuntimeException("Not supported");
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
    //LOG.info("Key {}, {}", num, taskId);
    return statefulTransforms.stream().map(t -> t.getNumKeys())
      .reduce((x, y) -> x + y)
      .get();
  }

  @Override
  public boolean isSource() {
    return false;
  }

  @Override
  public boolean isOffloadedTask() {
    return offloaded;
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
  public void initialize() {
    TaskExecutorUtil.sendInitMessage(task, inputPipeRegister);
  }

  private void prepare() {
    final long st = System.currentTimeMillis();

    LOG.info("Start to registering input output pipe {}", taskId);

    task.getTaskOutgoingEdges().forEach(edge -> {
      LOG.info("Task outgoing edge for {} {}", taskId, edge);
      final IRVertex src = edge.getSrcIRVertex();
      final IRVertex dst = edge.getDstIRVertex();
      taskOutgoingEdges.putIfAbsent(src.getId(), new LinkedList<>());
      taskOutgoingEdges.get(src.getId()).add(dst.getId());
      final Integer taskIndex = RuntimeIdManager.getIndexFromTaskId(task.getTaskId());

      // bidrectional !!
      final int parallelism = edge
        .getDstIRVertex().getPropertyValue(ParallelismProperty.class).get();

      final CommunicationPatternProperty.Value comm =
        edge.getPropertyValue(CommunicationPatternProperty.class).get();

      LOG.info("Registering pipe for output edges in {}, parallelism {}", taskId, parallelism);

      if (comm.equals(CommunicationPatternProperty.Value.OneToOne)
        || comm.equals(CommunicationPatternProperty.Value.TransientOneToOne)) {
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

      LOG.info("End of task outgoing edge for {} {}", taskId, edge);
    });

    LOG.info("Task {} registering pipe time: {}", taskId, System.currentTimeMillis() - st);


    final int taskIndex = RuntimeIdManager.getIndexFromTaskId(task.getTaskId());

    // Traverse in a reverse-topological order to ensure that each visited vertex's children vertices exist.
    final List<IRVertex> reverseTopologicallySorted = new ArrayList<>(irVertexDag.getTopologicalSort());
    Collections.reverse(reverseTopologicallySorted);

    // Build a map for edge as a key and edge index as a value
    // This variable is used for creating NextIntraTaskOperatorInfo
    // in {@link this#getInternalMainOutputs and this#internalMainOutputs}
    reverseTopologicallySorted.forEach(childVertex -> {

      if (childVertex.isStateful) {
        isStateless = false;
        if (childVertex instanceof OperatorVertex) {
          final OperatorVertex ov = (OperatorVertex) childVertex;
          statefulTransforms.add(ov.getTransform());
          LOG.info("Set GBK final transform");
        }
      }


      if (irVertexDag.getOutgoingEdgesOf(childVertex.getId()).size() == 0) {
        childVertex.isSink = true;

        /*
        // If it is sink or emit to next stage, we log the latency
        LOG.info("MonitoringVertex: {}", childVertex.getId());
        if (!samplingMap.containsKey(childVertex.getId())) {
          samplingMap.put(childVertex.getId(), 1.0);
        }
        */

        LOG.info("Sink vertex: {}", childVertex.getId());
      }
    });

    // serializedDag = SerializationUtils.serialize(irVertexDag);
    final Map<String, OutputCollector> outputCollectorMap = new HashMap<>();

    // Create a harness for each vertex
    reverseTopologicallySorted.forEach(irVertex -> {
        final Optional<Readable> sourceReader = TaskExecutorUtil
          .getSourceVertexReader(irVertex, task.getIrVertexIdToReadable());

        if (sourceReader.isPresent() != irVertex instanceof SourceVertex) {
          throw new IllegalStateException(irVertex.toString());
        }

        final OutputCollector outputCollector;
        if (irVertex.isStateful) {
          outputCollector = partialOutputCollector;
          vertexIdAndCollectorMap.put(irVertex.getId(), Pair.of(null, outputCollector));
        } else {
          outputCollector = outputCollectorGenerator
            .generate(irVertex,
              taskId,
              irVertexDag,
              this,
              serializerManager,
              samplingMap,
              vertexIdAndCollectorMap,
              taskMetrics,
              task.getTaskOutgoingEdges(),
              operatorInfoMap,
              TaskExecutorUtil.getExternalAdditionalOutputMap(
                irVertex, task.getTaskOutgoingEdges(), intermediateDataIOFactory, taskId,
                taskMetrics));
        }


      outputCollectorMap.put(irVertex.getId(), outputCollector);

        // Create VERTEX HARNESS
        final Transform.Context context = new TransformContextImpl(
          irVertex, serverlessExecutorProvider, taskId, stateStore,
          null,
          executorId,
          sharedObject);

        TaskExecutorUtil.prepareTransform(irVertex, context, outputCollector, taskId);

        // Prepare data READ
        // Source read
        // TODO[SLS]: should consider multiple outgoing edges
        // All edges will have the same encoder/decoder!
        if (irVertex instanceof SourceVertex) {
          // final RuntimeEdge edge = irVertexDag.getOutgoingEdgesOf(irVertex).get(0);
          final RuntimeEdge edge = task.getTaskOutgoingEdges().get(0);
          // srcSerializer = serializerManager.getSerializer(edge.getId());
          // LOG.info("SourceVertex: {}, edge: {}, serializer: {}", irVertex.getId(), edge.getId(),
          // srcSerializer);

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
            },
            offloaded);

          edgeToDataFetcherMap.put(edge.getId(), fe);

          sourceVertexDataFetchers.add(fe);
          allFetchers.add(fe);

          if (sourceVertexDataFetchers.size() > 1) {
            throw new RuntimeException("Source vertex data fetcher is larger than one");
          }
        }
      });

    LOG.info("End of source vertex prepare {}", taskId);

      // Parent-task read
      // TODO #285: Cache broadcasted data
      task.getTaskIncomingEdges()
        .stream()
        // .filter(inEdge -> inEdge.getDstIRVertex().getId().equals(irVertex.getId())) // edge to this vertex
        .map(incomingEdge -> {

          LOG.info("Incoming edge: {}, taskIndex: {}, taskId: {}", incomingEdge, taskIndex, taskId);

          return Pair.of(incomingEdge, intermediateDataIOFactory
            .createReader(
              taskId,
              incomingEdge.getSrcIRVertex(), incomingEdge, executorThreadQueue));
        })
        .forEach(pair -> {
          final String irVertexId = pair.left().getDstIRVertex().getId();
          final IRVertex irVertex = irVertexDag.getVertexById(irVertexId);

          if (irVertex instanceof OperatorVertex) {

            // LOG.info("Adding data fetcher for {} / {}", taskId, irVertex.getId());

            final StageEdge edge = pair.left();
            final InputReader parentTaskReader = pair.right();
            final OutputCollector dataFetcherOutputCollector =
              new DataFetcherOutputCollector(edge.getSrcIRVertex(), (OperatorVertex) irVertex,
                outputCollectorMap.get(irVertex.getId()), taskId);


            final DataFetcher df = new MultiThreadParentTaskDataFetcher(
              taskId,
              edge.getSrcIRVertex(),
              edge,
              dataFetcherOutputCollector);

            edgeToDataFetcherMap.put(edge.getId(), df);

            // LOG.info("Adding data fetcher 22 for {} / {}, parallelism {}",
            //  taskId, irVertex.getId(), parallelism);

            final CommunicationPatternProperty.Value comm =
              edge.getPropertyValue(CommunicationPatternProperty.class).get();

            final int parallelism = edge
              .getSrcIRVertex().getPropertyValue(ParallelismProperty.class).get();

            LOG.info("Registering pipe for input edges {} in {}, parallelism {}",
              edge.getId(), taskId, parallelism);

            if (comm.equals(CommunicationPatternProperty.Value.OneToOne)
              || comm.equals(CommunicationPatternProperty.Value.TransientOneToOne)) {
              inputPipeRegister.registerInputPipe(
                RuntimeIdManager.generateTaskId(edge.getSrc().getId(), taskIndex, 0),
                edge.getId(),
                task.getTaskId(),
                parentTaskReader);

              // LOG.info("Adding data fetcher 33 for {} / {}", taskId, irVertex.getId());
              // taskWatermarkManager.addDataFetcher(df.getEdgeId(), 1);
            } else {
              for (int i = 0; i < parallelism; i++) {
                inputPipeRegister.registerInputPipe(
                  RuntimeIdManager.generateTaskId(edge.getSrc().getId(), i, 0),
                  edge.getId(),
                  task.getTaskId(),
                  parentTaskReader);
              }
              // LOG.info("Adding data fetcher 44 for {} / {}", taskId, irVertex.getId());
              // taskWatermarkManager.addDataFetcher(df.getEdgeId(), parallelism);
            }


            allFetchers.add(df);

            // LOG.info("End of adding data fetcher for {} / {}", taskId, irVertex.getId());
          }
        });

    // For latency logging

    prepared.set(true);

    LOG.info("Task {} prepar time: {}", taskId, System.currentTimeMillis() - st);
  }

  /**
   * Process a data element down the DAG dependency.
   */
  private void processElement(final OutputCollector outputCollector, final TimestampAndValue dataElement) {

    final long ns = System.nanoTime();

    outputCollector.setInputTimestamp(dataElement.timestamp);
    outputCollector.emit(dataElement.value);

    final long endNs = System.nanoTime();

    taskMetrics.incrementComputation(endNs - ns);
  }

  private void processWatermark(final OutputCollector outputCollector,
                                final Watermark watermark) {
    outputCollector.emitWatermark(watermark);
  }

  public void setPairTaskStopped(boolean val) {
    LOG.info("Pair task of {} stopped {}", taskId, val);
    this.pairTaskStopped = val;
    if (pairTaskStopped) {
      partialOutputEmitter = pToLocalCombiner;
    } else {
      partialOutputEmitter = pToRemoteEmitter;
    }
  }

  public void clearState() {
    // TODO
    LOG.info("Clear state of {}", taskId);
    statefulTransforms.forEach(transform -> transform.clearState());
  }

  @Override
  public void handleData(final String edgeId,
                                final TaskHandlingEvent taskHandlingEvent) {
    // input
    taskMetrics.incrementInBytes(taskHandlingEvent.readableBytes());
    final long serializedStart = System.nanoTime();
    final Object data = taskHandlingEvent.getData();
    final long serializedEnd = System.nanoTime();

    taskMetrics.incrementDeserializedTime(serializedEnd - serializedStart);

    try {
      handleInternalData(edgeToDataFetcherMap.get(edgeId), data);
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Exception for task " + taskId + " in processing event edge " +
        edgeId + ", handling event " + taskHandlingEvent.getClass() + ", " +
        " event " + data.getClass() + ", " +
        ((TaskRelayDataEvent) taskHandlingEvent).remoteLocal + ", "
        + ((TaskRelayDataEvent) taskHandlingEvent).valueDecoderFactory);
    }
  }

  private void handleInternalData(final DataFetcher dataFetcher, Object event) {
    if (event instanceof Finishmark) {
      // We've consumed all the data from this data fetcher.
    } else if (event instanceof WatermarkWithIndex) {
      // Watermark
      // LOG.info("Handling watermark with index {}", event);
      final WatermarkWithIndex d = (WatermarkWithIndex) event;
      taskWatermarkManager.trackAndEmitWatermarks(
        taskId,
        dataFetcher.getEdgeId(), d.getIndex(), d.getWatermark().getTimestamp())
        .ifPresent(watermark -> {
          processWatermark(dataFetcher.getOutputCollector(), new Watermark(watermark));
        });
    } else if (event instanceof TimestampAndValue) {
      taskMetrics.incrementInputProcessElement();
      processElement(dataFetcher.getOutputCollector(), (TimestampAndValue) event);
    } else {
      throw new RuntimeException("Invalids event type " + event);
    }
  }

  // exeutor thread가 바로 부르는 method
  @Override
  public boolean handleSourceData() {
    throw new RuntimeException("not support");
  }

  ////////////////////////////////////////////// Transform-specific helper methods
  interface PartialOutputEmitter {
    void emit(Object output, long ts);
    void emitWatermark(Watermark watermark);
  }

  final class PartialOutputEmitToLocalFinal implements PartialOutputEmitter{

    @Override
    public void emit(Object output, long ts) {
      // send to final
//      if (taskId.contains("Stage17") || taskId.contains("Stage18")) {
//        LOG.info("Partial output to Local final for {}: {}", taskId, output);
//      }

//      if (taskId.contains("Stage6")) {
//        LOG.info("Partial output to local final for {}: ts: {}", taskId,
//          new Instant(ts));
//      }
      finalOutputCollector.setInputTimestamp(ts);
      pToFinalTransform.onData(output);
    }

    @Override
    public void emitWatermark(Watermark watermark) {
      // send to final
//      if (taskId.contains("Stage6")) {
//        LOG.info("Partial output watermark local final for {}: ts: {} {}", taskId,
//          new Instant(watermark.getTimestamp()));
//      }
      pToFinalTransform.onWatermark(watermark);
    }
  }

  final class PartialOutputEmitToRemoteMerger implements PartialOutputEmitter {

    @Override
    public void emit(Object output, long ts) {
//      if (taskId.contains("Stage6")) {
//        LOG.info("Partial output to remote (partial) for {}: ts: {} {}", taskId,
//          new Instant(ts),
//          output);
//      }
      pipeManagerWorker.writeData(taskId,
        mergerEdgeId, mergerTaskId, serializer,
        new TimestampAndValue<>(ts, output));
    }

    @Override
    public void emitWatermark(Watermark watermark) {
//      if (taskId.contains("Stage6")) {
//        LOG.info("Partial output watermark remote (partial) for {}: ts: {} {}", taskId,
//          new Instant(watermark.getTimestamp()));
//      }
      pipeManagerWorker.writeData(taskId,
        mergerEdgeId, mergerTaskId, serializer, new WatermarkWithIndex(watermark, taskIndex));
    }
  }


  public void setIRVertexPutOnHold(final IRVertex irVertex) {
  }


  private WatermarkTracker getTaskWatermarkManager() {
    if (task.getTaskIncomingEdges().size() == 0) {
      return null;
    } else if (task.getTaskIncomingEdges().size() == 1) {
      final int parallelism = task.getTaskIncomingEdges().get(0)
        .getSrcIRVertex().getPropertyValue(ParallelismProperty.class).get();

      LOG.info("Registering pipe for input edges {} in {}, parallelism {}",
        task.getTaskIncomingEdges().get(0).getId(), taskId, parallelism);

      final CommunicationPatternProperty.Value comm = task.getTaskIncomingEdges().get(0)
        .getDataCommunicationPattern();
      return new SingleStageWatermarkTracker(getWatermarkParllelism(parallelism, comm));

    } else if (task.getTaskIncomingEdges().size() == 2) {
      final StageEdge firstEdge = task.getTaskIncomingEdges().get(0);
      final StageEdge secondEdge = task.getTaskIncomingEdges().get(1);
      final int firstParallelism = ((StageEdge) firstEdge).getSrcIRVertex().getPropertyValue(ParallelismProperty.class)
        .get();
      final int secondParallelism = ((StageEdge) secondEdge).getSrcIRVertex().getPropertyValue(ParallelismProperty.class)
        .get();
      final CommunicationPatternProperty.Value firstComm = firstEdge
        .getDataCommunicationPattern();
       final CommunicationPatternProperty.Value secondComm = secondEdge
        .getDataCommunicationPattern();

       return new DoubleStageWatermarkTracker(
         firstEdge.getId(),
         getWatermarkParllelism(firstParallelism, firstComm),
         secondEdge.getId(),
         getWatermarkParllelism(secondParallelism, secondComm));
    } else {
      throw new RuntimeException("Not support incoming edge >= 3 " + taskId + ", " +  task.getTaskIncomingEdges());
    }
  }

  private int getWatermarkParllelism(final int parallelism,
                                     final CommunicationPatternProperty.Value comm) {
    if (comm.equals(CommunicationPatternProperty.Value.OneToOne)
      || comm.equals(CommunicationPatternProperty.Value.TransientOneToOne)) {
      return 1;
    } else {
      return parallelism;
    }
  }

  private Optional<WatermarkTracker> restoreTaskInputWatermarkManager() {
   if (stateStore.containsState(taskId + "-taskWatermarkManager")) {
      try {
        final DataInputStream is = new DataInputStream(
          stateStore.getStateStream(taskId + "-taskWatermarkManager"));

        if (task.getTaskIncomingEdges().size() == 0) {
          return Optional.empty();
        } else if (task.getTaskIncomingEdges().size() == 1) {
          return Optional.of(SingleStageWatermarkTracker.decode(taskId, is));
        } else if (task.getTaskIncomingEdges().size() == 2) {
          return Optional.of(DoubleStageWatermarkTracker.decode(taskId, is));
        } else {
          throw new RuntimeException("Not supported edge > 2" + taskId);
        }
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
    return Optional.empty();
  }

  @Override
  public void restore() {
    throw new RuntimeException("SMTask not support restore " + taskId);
    // stateRestore(taskId);
  }

  @Override
  public boolean checkpoint(final boolean checkpointSource,
                            final String checkpointId) {
    throw new RuntimeException("SMTask not support checkpoint " + taskId);
    // stateMigration(checkpointId);
    // return true;
  }

  @Override
  public String toString() {
    return taskId;
  }
}
