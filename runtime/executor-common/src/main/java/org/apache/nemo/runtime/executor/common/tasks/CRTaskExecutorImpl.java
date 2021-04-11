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
import org.apache.nemo.common.ir.vertex.utility.ConditionalRouterVertex;
import org.apache.nemo.common.punctuation.*;
import org.apache.nemo.offloading.common.ServerlessExecutorProvider;
import org.apache.nemo.offloading.common.StateStore;
import org.apache.nemo.offloading.common.TaskHandlingEvent;
import org.apache.nemo.runtime.executor.common.*;
import org.apache.nemo.runtime.executor.common.datatransfer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Executes a task.
 * Should be accessed by a single thread.
 */
@NotThreadSafe
public final class CRTaskExecutorImpl implements TaskExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(CRTaskExecutorImpl.class.getName());

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

  private TaskInputWatermarkManager taskWatermarkManager;
  private final InputPipeRegister inputPipeRegister;

  // private final OffloadingManager offloadingManager;

  private final PipeManagerWorker pipeManagerWorker;

  private final Map<String, DataFetcher> edgeToDataFetcherMap = new HashMap<>();

  private final OutputCollectorGenerator outputCollectorGenerator;

  private final boolean offloaded;

  private final List<ConditionalRouterVertex> crVertices;

  private final Transform.ConditionalRouting conditionalRouting;

  private final boolean singleOneToOneInput;

  final Map<String, List<OutputWriter>> externalAdditionalOutputMap;

  // THIS TASK SHOULD NOT BE MOVED !!
  // THIS TASK SHOULD NOT BE MOVED !!
  // THIS TASK SHOULD NOT BE MOVED !!

  private enum CRTaskState {
    STATE_MIGRATION_TO_REMOTE,
    STATE_MIGRATION_FROM_REMOTE,
    NORMAL,
    SEND_DATA_TO_REMOTE,
  }

  private CRTaskState currState = CRTaskState.NORMAL;

  /**
   * Constructor.
   *
   * @param task                   Task with information needed during execution.
   * @param irVertexDag            A DAG of vertices.
   * @param intermediateDataIOFactory    For reading from/writing to data to other tasks.
   */
  public CRTaskExecutorImpl(final long threadId,
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
    this.conditionalRouting = conditionalRouting;
    this.outputCollectorGenerator = outputCollectorGenerator;
    this.pipeManagerWorker = pipeManagerWorker;
    // this.offloadingManager = offloadingManager;
    this.stateStore = stateStore;
    this.crVertices = new ArrayList<>();
    this.taskMetrics = new TaskMetrics();
    this.executorThreadQueue = executorThreadQueue;
    //LOG.info("Copied outgoing edges: {}, bytes: {}", copyOutgoingEdges);
    this.prepareService = prepareService;
    this.inputPipeRegister = inputPipeRegister;
    this.taskId = task.getTaskId();

    this.externalAdditionalOutputMap = new HashMap<>();

    this.statefulTransforms = new ArrayList<>();

    final long restoresSt = System.currentTimeMillis();

    this.singleOneToOneInput = task.getTaskIncomingEdges().size() == 1
      && (task.getTaskIncomingEdges().get(0).getDataCommunicationPattern()
      .equals(CommunicationPatternProperty.Value.OneToOne) ||
      task.getTaskIncomingEdges().get(0).getDataCommunicationPattern()
        .equals(CommunicationPatternProperty.Value.TransientOneToOne));

    this.taskWatermarkManager = restoreTaskInputWatermarkManager().orElse(new TaskInputWatermarkManager());
    LOG.info("Task {} watermark manager restore time {}", taskId, System.currentTimeMillis() - restoresSt);

    this.threadId = threadId;
    this.executorId = executorId;
    this.sourceVertexDataFetchers = new ArrayList<>();
    this.task = task;
    this.irVertexDag = irVertexDag;
    this.vertexIdAndCollectorMap = new HashMap<>();
    this.taskOutgoingEdges = new HashMap<>();
    this.samplingMap = samplingMap;


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

    // samplingMap.putAll(evalConf.samplingJson);

    this.serverlessExecutorProvider = serverlessExecutorProvider;

    this.serializerManager = serializerManager;

    // TODO: Initialize states for the task
    // TODO: restart output writers and sources if it is moved

    // Prepare data structures
    final long st1 = System.currentTimeMillis();
    prepare(task, irVertexDag, intermediateDataIOFactory);

    // offloadingPreparer.prepare(taskId, bytes);

    LOG.info("Task {} prepar time: {}", taskId, System.currentTimeMillis() - st1);
    prepared.set(true);

    LOG.info("Source vertex data fetchers in defaultTaskExecutorimpl: {}", sourceVertexDataFetchers);

    /*
    pollingTrigger.scheduleAtFixedRate(() -> {
      pollingTime = true;
    }, pollingInterval, pollingInterval, TimeUnit.MILLISECONDS);
    */

    if (isLocalSource) {
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

      if (childVertex instanceof ConditionalRouterVertex) {
        crVertices.add((ConditionalRouterVertex)childVertex);
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

      externalAdditionalOutputMap.putAll(
        TaskExecutorUtil.getExternalAdditionalOutputMap(
          irVertex, task.getTaskOutgoingEdges(), intermediateDataIOFactory, taskId,
          taskMetrics));

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
            externalAdditionalOutputMap);

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

            final int parallelism = edge
              .getSrcIRVertex().getPropertyValue(ParallelismProperty.class).get();

            final CommunicationPatternProperty.Value comm =
              edge.getPropertyValue(CommunicationPatternProperty.class).get();

            final DataFetcher df = new MultiThreadParentTaskDataFetcher(
              taskId,
              edge.getSrcIRVertex(),
              edge,
              dataFetcherOutputCollector);

            edgeToDataFetcherMap.put(edge.getId(), df);

            // LOG.info("Adding data fetcher 22 for {} / {}, parallelism {}",
            //  taskId, irVertex.getId(), parallelism);

            LOG.info("Registering pipe for input edges in {}, parallelism {}", taskId, parallelism);

            if (comm.equals(CommunicationPatternProperty.Value.OneToOne)
              || comm.equals(CommunicationPatternProperty.Value.TransientOneToOne)) {
              inputPipeRegister.registerInputPipe(
                RuntimeIdManager.generateTaskId(edge.getSrc().getId(), taskIndex, 0),
                edge.getId(),
                task.getTaskId(),
                parentTaskReader);

              // LOG.info("Adding data fetcher 33 for {} / {}", taskId, irVertex.getId());
              taskWatermarkManager.addDataFetcher(df.getEdgeId(), 1);
            } else {
              for (int i = 0; i < parallelism; i++) {
                inputPipeRegister.registerInputPipe(
                  RuntimeIdManager.generateTaskId(edge.getSrc().getId(), i, 0),
                  edge.getId(),
                  task.getTaskId(),
                  parentTaskReader);
              }
              // LOG.info("Adding data fetcher 44 for {} / {}", taskId, irVertex.getId());
              taskWatermarkManager.addDataFetcher(df.getEdgeId(), parallelism);
            }


            allFetchers.add(df);

            // LOG.info("End of adding data fetcher for {} / {}", taskId, irVertex.getId());
          }
        });
    // return sortedHarnessList;
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

  private final Queue<Pair<String, TaskHandlingEvent>> pendingQueue = new LinkedBlockingQueue<>();

  // For state migration from VM -> Lambda or Lambda -> VM task
  public void prepareStateMigration() {
    currState = CRTaskState.STATE_MIGRATION_TO_REMOTE;
    checkpoint(false);
  }

  public void prepareStateGetFromRemote() {
    currState = CRTaskState.STATE_MIGRATION_FROM_REMOTE;
  }

  public void getStateFromRemote() {
    if (currState != CRTaskState.STATE_MIGRATION_FROM_REMOTE) {
      throw new RuntimeException("Invalid curr state " + currState);
    }

    restore();
    currState = CRTaskState.NORMAL;
    // flush buffer
    pendingQueue.forEach(data -> {
      handleData(data.left(), data.right());
    });
  }

  public void finishStateMigration() {
    if (currState != CRTaskState.STATE_MIGRATION_TO_REMOTE) {
      throw new RuntimeException("Invalid curr state " + currState);
    }

    currState = CRTaskState.SEND_DATA_TO_REMOTE;
    // flush buffer
    pendingQueue.forEach(data -> {
      // Send to remote additional tag output writer
      /*
      externalAdditionalOutputMap.get(Util.PARTIAL_RR_TAG)
        .forEach(writer -> {
          writer.wri
        });
      TODO
      */
    });
    pendingQueue.clear();
  }

  @Override
  public void handleData(final String edgeId,
                         final TaskHandlingEvent taskHandlingEvent) {
    switch (currState) {
      case NORMAL: {
        // input
        taskMetrics.incrementInBytes(taskHandlingEvent.readableBytes());
        final long serializedStart = System.nanoTime();
        final Object data = taskHandlingEvent.getData();
        final long serializedEnd = System.nanoTime();

        taskMetrics.incrementDeserializedTime(serializedEnd - serializedStart);

        // LOG.info("Handling data for task {}, index {}, watermark {}",
        //  taskId, taskHandlingEvent.getInputPipeIndex(), data instanceof WatermarkWithIndex);

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
        break;
      }
      case STATE_MIGRATION_TO_REMOTE:
      case STATE_MIGRATION_FROM_REMOTE: {
        pendingQueue.add(Pair.of(edgeId, taskHandlingEvent));
        break;
      }
      case SEND_DATA_TO_REMOTE: {
        // Send to remote
        // TODO
        break;
      }
      default: {
        throw new RuntimeException("Not supported " + currState);
      }
    }

  }

  private void handleInternalData(final DataFetcher dataFetcher, Object event) {
    if (event instanceof Finishmark) {
      // We've consumed all the data from this data fetcher.
    } else if (event instanceof WatermarkWithIndex) {
      // Watermark
      // LOG.info("Handling watermark with index {}", event);
      final WatermarkWithIndex d = (WatermarkWithIndex) event;

      if (singleOneToOneInput) {
        processWatermark(dataFetcher.getOutputCollector(), d.getWatermark());
      } else {
        final Optional<Watermark> watermark =
          taskWatermarkManager.updateWatermark(dataFetcher.getEdgeId(), d.getIndex(), d.getWatermark().getTimestamp());

        if (watermark.isPresent()) {
          // LOG.info("Emitting watermark for {} / {}", taskId, new Instant(watermark.get().getTimestamp()));
          processWatermark(dataFetcher.getOutputCollector(), watermark.get());
        }
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
//      if (!isSource()) {
//        final OperatorVertex ov = ((DataFetcherOutputCollector) dataFetcher.getOutputCollector()).getNextOperatorVertex();
//        LOG.info("Processing element for task {}, of operator {}, transform {}", taskId, ov.getId(), ov.getTransform());
//      }
      processElement(dataFetcher.getOutputCollector(), (TimestampAndValue) event);
    } else {
      throw new RuntimeException("Invalids event type " + event);
      /*
      // input for streaming vertex !!
      final long ns = System.nanoTime();
      dataFetcher.getOutputCollector().emit(event);
      final long endNs = System.nanoTime();
      taskMetrics.incrementComputation(endNs - ns);
      */
    }
  }

  // exeutor thread가 바로 부르는 method
  @Override
  public boolean handleSourceData() {
    throw new RuntimeException("not support");
  }



  ////////////////////////////////////////////// Transform-specific helper methods


  public void setIRVertexPutOnHold(final IRVertex irVertex) {
  }

  private boolean finished = false;

  private Optional<TaskInputWatermarkManager> restoreTaskInputWatermarkManager() {
    if (stateStore.containsState(taskId + "-taskWatermarkManager")) {
      try {
        final InputStream is = stateStore.getStateStream(taskId + "-taskWatermarkManager");
        final TaskInputWatermarkManager tm = TaskInputWatermarkManager.decode(is);
        return Optional.of(tm);
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
    return Optional.empty();
  }

  private void restore() {
    try {
      final InputStream is = stateStore.getStateStream(taskId + "-taskWatermarkManager");
      taskWatermarkManager = TaskInputWatermarkManager.decode(is);
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    if (!isStateless) {
      statefulTransforms.forEach(transform -> transform.restore());
    }
  }

  @Override
  public boolean checkpoint(final boolean checkpointSource) {
    final OutputStream os = stateStore.getOutputStream(taskId + "-taskWatermarkManager");
    try {
      taskWatermarkManager.encode(os);
      os.close();
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    if (!isStateless) {
      statefulTransforms.forEach(transform -> transform.checkpoint());
    }

    return true;
  }


  @Override
  public String toString() {
    return taskId;
  }
}
