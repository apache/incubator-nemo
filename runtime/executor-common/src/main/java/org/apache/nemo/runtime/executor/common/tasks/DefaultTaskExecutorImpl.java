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

import org.apache.nemo.common.*;
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
import org.apache.nemo.common.punctuation.*;
import org.apache.nemo.offloading.common.ServerlessExecutorProvider;
import org.apache.nemo.offloading.common.TaskHandlingEvent;
import org.apache.nemo.runtime.executor.common.*;
import org.apache.nemo.runtime.executor.common.datatransfer.*;
import org.apache.nemo.offloading.common.StateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

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

  public enum CurrentState {
    RUNNING,
    WAIT_WORKER,
    OFFLOAD_PENDING,
    OFFLOADED,
    DEOFFLOAD_PENDING
  }

  private CurrentState currentState = CurrentState.RUNNING;

  // private final OffloadingManager offloadingManager;

  private final PipeManagerWorker pipeManagerWorker;

  private final Map<String, DataFetcher> edgeToDataFetcherMap = new HashMap<>();

  private final OutputCollectorGenerator outputCollectorGenerator;

  private final boolean offloaded;

  private final Transform.ConditionalRouting conditionalRouting;

  private final boolean singleOneToOneInput;

  private final IntermediateDataIOFactory intermediateDataIOFactory;

  /**
   * Constructor.
   *
   * @param task                   Task with information needed during execution.
   * @param irVertexDag            A DAG of vertices.
   * @param intermediateDataIOFactory    For reading from/writing to data to other tasks.
   */
  public DefaultTaskExecutorImpl(final long threadId,
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
    this.task = task;
    this.offloaded = offloaded;
    this.intermediateDataIOFactory = intermediateDataIOFactory;
    this.conditionalRouting = conditionalRouting;
    this.outputCollectorGenerator = outputCollectorGenerator;
    this.pipeManagerWorker = pipeManagerWorker;
    // this.offloadingManager = offloadingManager;
    this.stateStore = stateStore;
    this.taskMetrics = new TaskMetrics();
    this.executorThreadQueue = executorThreadQueue;
    //LOG.info("Copied outgoing edges: {}, bytes: {}", copyOutgoingEdges);
    this.prepareService = prepareService;
    this.inputPipeRegister = inputPipeRegister;
    this.taskId = task.getTaskId();

    this.statefulTransforms = new ArrayList<>();

    final long restoresSt = System.currentTimeMillis();

    this.taskWatermarkManager = restoreTaskInputWatermarkManager().orElse(getTaskWatermarkManager());

    this.singleOneToOneInput = task.getTaskIncomingEdges().size() == 1
      && (task.getTaskIncomingEdges().get(0).getDataCommunicationPattern()
      .equals(CommunicationPatternProperty.Value.OneToOne) ||
      task.getTaskIncomingEdges().get(0).getDataCommunicationPattern()
        .equals(CommunicationPatternProperty.Value.TransientOneToOne));

    LOG.info("Task {} watermark manager restore time {}", taskId, System.currentTimeMillis() - restoresSt);

    this.threadId = threadId;
    this.executorId = executorId;
    this.sourceVertexDataFetchers = new ArrayList<>();
    this.irVertexDag = irVertexDag;
    this.vertexIdAndCollectorMap = new HashMap<>();
    this.taskOutgoingEdges = new HashMap<>();
    this.samplingMap = samplingMap;
    this.serverlessExecutorProvider = serverlessExecutorProvider;
    this.serializerManager = serializerManager;
    LOG.info("Source vertex data fetchers in defaultTaskExecutorimpl: {}", sourceVertexDataFetchers);

    prepare();

    if (isLocalSource) {
      this.adjustTime = System.currentTimeMillis() - 1436918400000L;
    } else {
      this.adjustTime = 0;
    }
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


  // per second
  private final AtomicLong throttleSourceRate =  new AtomicLong(1000000);
  private long processedSourceData = 0;
  private long prevSourceTrackTime = System.currentTimeMillis();

  @Override
  public void setThrottleSourceRate(final long num) {
    throttleSourceRate.set(num);
  }

  @Override
  public CurrentState getStatus() {
    return currentState;
  }

  // For source task
  @Override
  public boolean hasData() {

    final long curr = System.currentTimeMillis();
    if (curr - prevSourceTrackTime >= 5) {
      final long elapsed = curr - prevSourceTrackTime;
      if (processedSourceData * (1000 / (double)elapsed) > throttleSourceRate.get()) {
        // Throttle !!
        return false;
      } else {
        prevSourceTrackTime = curr;
        processedSourceData = 0;
      }
    }

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
      return statefulTransforms.stream().map(t -> t.getNumKeys())
        .reduce((x, y) -> x + y)
        .get();
    }
  }

  @Override
  public boolean isSource() {
    return sourceVertexDataFetchers.size() > 0;
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
   * @return fetchers and harnesses.
   */
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

      final List<String> dstTaskIds = TaskExecutorUtil.getDstTaskIds(taskId, edge);
        dstTaskIds.forEach(dstTaskId -> {
          inputPipeRegister.registerInputPipe(
            dstTaskId,
            edge.getId(),
            task.getTaskId(),
            new PipeInputReader(edge.getDstIRVertex(), taskId, (RuntimeEdge) edge,
              serializerManager.getSerializer(((RuntimeEdge)edge).getId()), executorThreadQueue));
        });

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

      if (childVertex instanceof OperatorVertex) {
        LOG.info("Vertex {} isGBK {} isPushback {}, {}", childVertex.getId(),
          childVertex.isGBK, childVertex.isPushback, ((OperatorVertex)childVertex).getTransform());
      }

      if (childVertex.isGBK || childVertex.isPushback) {
        isStateless = false;
        if (childVertex instanceof OperatorVertex) {
          final OperatorVertex ov = (OperatorVertex) childVertex;
          LOG.info("Vertex {} transform {}", ov.getTransform());
          statefulTransforms.add(ov.getTransform());
          LOG.info("Set final transform {}", ov.getTransform());
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

      final OutputCollector outputCollector = outputCollectorGenerator
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

      outputCollectorMap.put(irVertex.getId(), outputCollector);

      // Create VERTEX HARNESS
      final Transform.Context context = new TransformContextImpl(
        irVertex, serverlessExecutorProvider, taskId, stateStore,
        conditionalRouting,
        executorId);

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

        /*
        // Register input pipe for offloaded source !!
        if (offloaded) {
          inputPipeRegister.registerInputPipe(
            "Origin",
            edge.getId(),
            taskId,
            intermediateDataIOFactory
              .createReader(
                taskId,
                irVertex, edge, executorThreadQueue));
        } else {
          inputPipeRegister.retrieveIndexForOffloadingSource(taskId, edge.getId());
        }
        */

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

          TaskExecutorUtil.getSrcTaskIds(taskId, edge).forEach(srcTaskId -> {
            inputPipeRegister.registerInputPipe(
              srcTaskId,
              edge.getId(),
              task.getTaskId(),
              parentTaskReader);
          });

          allFetchers.add(df);

          // LOG.info("End of adding data fetcher for {} / {}", taskId, irVertex.getId());
        }
      });

    // For latency logging
    for (final Pair<OperatorMetricCollector, OutputCollector> metricCollector :
      vertexIdAndCollectorMap.values()) {
      metricCollector.left().setAdjustTime(adjustTime);
    }

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

  @Override
  public void handleData(final String edgeId,
                         final TaskHandlingEvent taskHandlingEvent) {
    // input
    taskMetrics.incrementInBytes(taskHandlingEvent.readableBytes());
    final long serializedStart = System.nanoTime();
    final Object data = taskHandlingEvent.getData();
    final long serializedEnd = System.nanoTime();

    taskMetrics.incrementDeserializedTime(serializedEnd - serializedStart);

    // LOG.info("Handling data for task {}, index {}, watermark {}",
    //  taskId, taskHandlingEvent.getRemoteInputPipeIndex(), data instanceof WatermarkWithIndex);

    try {
      handleInternalData(edgeToDataFetcherMap.get(edgeId), data);
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Exception for task " + taskId + " in processing event edge " +
        edgeId + ", handling event " + taskHandlingEvent.getClass() + ", " +
        " event " + data.getClass() + ", ");
    }
  }

  private void handleInternalData(final DataFetcher dataFetcher, Object event) {
    if (event instanceof Finishmark) {
      // We've consumed all the data from this data fetcher.
    } else if (event instanceof WatermarkWithIndex) {
      // Watermark
      final WatermarkWithIndex d = (WatermarkWithIndex) event;
      taskWatermarkManager.trackAndEmitWatermarks(
        taskId,
        dataFetcher.getEdgeId(), d.getIndex(), d.getWatermark().getTimestamp())
        .ifPresent(watermark -> {
          taskMetrics.setInputWatermark(watermark);
          processWatermark(dataFetcher.getOutputCollector(), new Watermark(watermark));
        });
    } else if (event instanceof Watermark) {
      // This MUST BE generated from input source
      if (!isSource()) {
        throw new RuntimeException("Invalid watermark !! this task is not source " + taskId);
      }
      processWatermark(sourceVertexDataFetchers.get(0).getOutputCollector(), (Watermark) event);
    } else if (event instanceof TimestampAndValue) {
      // This MUST BE generated from remote source
      taskMetrics.incrementInputProcessElement();
      processElement(dataFetcher.getOutputCollector(), (TimestampAndValue) event);
    } else {
      throw new RuntimeException("Invalids event type " + event);
    }
  }

  // exeutor thread가 바로 부르는 method
  @Override
  public boolean handleSourceData() {
    boolean processed = false;
    for (final SourceVertexDataFetcher dataFetcher : sourceVertexDataFetchers) {
      final Object event = dataFetcher.fetchDataElement();
      processedSourceData += 1;
      if (!event.equals(EmptyElement.getInstance())) {
        handleInternalData(dataFetcher, event);
        processed = true;
        break;
      }
    }
    return processed;
  }



  ////////////////////////////////////////////// Transform-specific helper methods

  public void setIRVertexPutOnHold(final IRVertex irVertex) {
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
    final long st = System.currentTimeMillis();
    try {
      final DataInputStream is = new DataInputStream(stateStore.getStateStream(taskId + "-taskWatermarkManager"));
      if (task.getTaskIncomingEdges().size() == 0) {
        taskWatermarkManager = null;
      } else if (task.getTaskIncomingEdges().size() == 1) {
        taskWatermarkManager = SingleStageWatermarkTracker.decode(taskId, is);
      } else if (task.getTaskIncomingEdges().size() == 2) {
        taskWatermarkManager = DoubleStageWatermarkTracker.decode(taskId, is);
      } else {
        throw new RuntimeException("Not supported edge > 2" + taskId);
      }
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    if (!offloaded) {
      for (final DataFetcher dataFetcher : allFetchers) {
        if (dataFetcher instanceof SourceVertexDataFetcher) {
          final SourceVertexDataFetcher srcDataFetcher = (SourceVertexDataFetcher) dataFetcher;
          final Readable readable = srcDataFetcher.getReadable();
          readable.restore();
        }
      }
    }

    if (!isStateless) {
      statefulTransforms.forEach(transform -> transform.restore(taskId));
    }

    final long et = System.currentTimeMillis();
    LOG.info("Restore state time of {}: {}", taskId, et - st);
  }

  @Override
  public boolean checkpoint(final boolean checkpointSource,
                            final String checkpointId) {
    final long st = System.currentTimeMillis();
    boolean hasChekpoint = false;

    // final byte[] bytes = FSTSingleton.getInstance().asByteArray(taskWatermarkManager);
    // stateStore.put(taskId + "-taskWatermarkManager", bytes);

    try (final DataOutputStream os = new DataOutputStream(
      stateStore.getOutputStream(checkpointId + "-taskWatermarkManager"))) {
      if (task.getTaskIncomingEdges().size() == 0) {
        // do nothing
      } else if (task.getTaskIncomingEdges().size() == 1) {
        ((SingleStageWatermarkTracker) taskWatermarkManager).encode(taskId, os);
      } else if (task.getTaskIncomingEdges().size() == 2) {
        ((DoubleStageWatermarkTracker) taskWatermarkManager).encode(taskId, os);
      } else {
        throw new RuntimeException("Not supported edge > 2" + taskId);
      }
    } catch (final IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    if (checkpointSource) {
      // Do not checkpoint source if it is offloaded
      // because source data will be redirected freom the origin executor
      for (final DataFetcher dataFetcher : allFetchers) {
        if (dataFetcher instanceof SourceVertexDataFetcher) {
          if (hasChekpoint) {
            throw new RuntimeException("Double checkpoint..." + taskId);
          }

          hasChekpoint = true;
          final SourceVertexDataFetcher srcDataFetcher = (SourceVertexDataFetcher) dataFetcher;
          final Readable readable = srcDataFetcher.getReadable();
          LOG.info("Checkpointing readable for task {}", taskId);
          readable.checkpoint();
          LOG.info("End of Checkpointing readable for task {}", taskId);
        }

        try {
          LOG.info("Closing data fetcher for task {}", taskId);
          dataFetcher.close();
          LOG.info("End of Closing data fetcher for task {}", taskId);
        } catch (Exception e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
    }


    if (!isStateless) {
      statefulTransforms.forEach(transform -> transform.checkpoint(checkpointId));
    }

    final long et = System.currentTimeMillis();
    LOG.info("Checkpoint elapsed time of {}: {}", taskId, et - st);

    return true;
  }


  @Override
  public String toString() {
    return taskId;
  }
}
