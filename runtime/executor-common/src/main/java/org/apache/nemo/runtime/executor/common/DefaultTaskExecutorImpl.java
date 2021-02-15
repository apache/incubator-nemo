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

import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.TaskMetrics;
import org.apache.nemo.common.coder.FSTSingleton;
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
import org.apache.nemo.common.punctuation.EmptyElement;
import org.apache.nemo.common.punctuation.Finishmark;
import org.apache.nemo.common.punctuation.TimestampAndValue;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.offloading.common.ServerlessExecutorProvider;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.Task;
import org.apache.nemo.offloading.common.TaskHandlingEvent;
import org.apache.nemo.runtime.executor.common.datatransfer.*;
import org.apache.nemo.offloading.common.StateStore;
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

  private final AtomicInteger processedCnt = new AtomicInteger(0);

  private boolean isStateless = true;

  private final String executorId;

  private final long threadId;

  private final List<DataFetcher> allFetchers = new ArrayList<>();

  private final AtomicLong taskExecutionTime = new AtomicLong(0);

  private final ExecutorService prepareService;

  private final ExecutorThreadQueue executorThreadQueue;

  private final AtomicBoolean prepared = new AtomicBoolean(false);

  private Transform statefulTransform;

  private final TaskMetrics taskMetrics;

  private final StateStore stateStore;

  private TaskInputWatermarkManager taskWatermarkManager;
  private final InputPipeRegister inputPipeRegister;

  private enum CurrentState {
    RUNNING,
    WAIT_WORKER,
    OFFLOAD_PENDING,
    OFFLOADED,
    DEOFFLOAD_PENDING
  }

  private CurrentState currentState = CurrentState.RUNNING;

  private final OffloadingManager offloadingManager;

  private final PipeManagerWorker pipeManagerWorker;

  private final Map<String, DataFetcher> edgeToDataFetcherMap = new HashMap<>();

  private final OutputCollectorGenerator outputCollectorGenerator;

  private final boolean offloaded;

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
                                 final OffloadingManager offloadingManager,
                                 final PipeManagerWorker pipeManagerWorker,
                                 final OutputCollectorGenerator outputCollectorGenerator,
                                 final boolean offloaded) {
    // Essential information
    //LOG.info("Non-copied outgoing edges: {}", task.getTaskOutgoingEdges());
    this.offloaded = offloaded;
    this.outputCollectorGenerator = outputCollectorGenerator;
    this.pipeManagerWorker = pipeManagerWorker;
    this.offloadingManager = offloadingManager;
    this.stateStore = stateStore;
    this.taskMetrics = new TaskMetrics();
    this.executorThreadQueue = executorThreadQueue;
    //LOG.info("Copied outgoing edges: {}, bytes: {}", copyOutgoingEdges);
    this.prepareService = prepareService;
    this.inputPipeRegister = inputPipeRegister;
    this.taskId = task.getTaskId();

    final long restoresSt = System.currentTimeMillis();
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

    LOG.info("Task {} registering pipe time: {}", taskId, System.currentTimeMillis() - st);

    // samplingMap.putAll(evalConf.samplingJson);

    this.serverlessExecutorProvider = serverlessExecutorProvider;

    this.serializerManager = serializerManager;

    // TODO: Initialize states for the task
    // TODO: restart output writers and sources if it is moved

    // Prepare data structures
    final long st1 = System.currentTimeMillis();
    prepare(task, irVertexDag, intermediateDataIOFactory);

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
  public boolean isOffloadedTask() {
    return offloaded;
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
    final List<IRVertex> reverseTopologicallySorted = irVertexDag.getTopologicalSort();
    Collections.reverse(reverseTopologicallySorted);

    // Build a map for edge as a key and edge index as a value
    // This variable is used for creating NextIntraTaskOperatorInfo
    // in {@link this#getInternalMainOutputs and this#internalMainOutputs}
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
          operatorInfoMap);

      // Create VERTEX HARNESS
      final Transform.Context context =  new TransformContextImpl(
          irVertex, serverlessExecutorProvider, taskId, stateStore);

      TaskExecutorUtil.prepareTransform(irVertex, context, outputCollector);

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

      // Parent-task read
      // TODO #285: Cache broadcasted data
      task.getTaskIncomingEdges()
        .stream()
        .filter(inEdge -> inEdge.getDstIRVertex().getId().equals(irVertex.getId())) // edge to this vertex
        .map(incomingEdge -> {

          LOG.info("Incoming edge: {}, taskIndex: {}, taskId: {}", incomingEdge, taskIndex, taskId);

          return Pair.of(incomingEdge, intermediateDataIOFactory
            .createReader(
              taskId,
              incomingEdge.getSrcIRVertex(), incomingEdge, executorThreadQueue));
        })
        .forEach(pair -> {
          if (irVertex instanceof OperatorVertex) {

            final StageEdge edge = pair.left();
            final InputReader parentTaskReader = pair.right();
            final OutputCollector dataFetcherOutputCollector =
              new DataFetcherOutputCollector(edge.getSrcIRVertex(), (OperatorVertex) irVertex,
                outputCollector, taskId);

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

              if (comm.equals(CommunicationPatternProperty.Value.OneToOne)) {
                inputPipeRegister.registerInputPipe(
                  RuntimeIdManager.generateTaskId(edge.getSrc().getId(), taskIndex, 0),
                  edge.getId(),
                  task.getTaskId(),
                  parentTaskReader);

                taskWatermarkManager.addDataFetcher(df.getEdgeId(), 1);

              } else {
                for (int i = 0; i < parallelism; i++) {
                  inputPipeRegister.registerInputPipe(
                    RuntimeIdManager.generateTaskId(edge.getSrc().getId(), i, 0),
                    edge.getId(),
                    task.getTaskId(),
                    parentTaskReader);
                }

                taskWatermarkManager.addDataFetcher(df.getEdgeId(), parallelism);
              }

              allFetchers.add(df);
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

  private final List<TaskHandlingEvent> bufferedData = new LinkedList<>();
  // private final List<Object> bufferedSourceData = new LinkedList<>();

  private void flushBuffer() {
    if (!bufferedData.isEmpty()) {
      // flush buffered data
      if (currentState.equals(CurrentState.OFFLOADED)) {

        // Write to offloaded task
        bufferedData.forEach(e ->
          offloadingManager.offloadIntermediateData(taskId, e));
        bufferedData.clear();

      } else if (currentState.equals(CurrentState.RUNNING)) {

        bufferedData.forEach(e -> handleInternalData(
          edgeToDataFetcherMap.get(e.getEdgeId()), e.getData()));
        bufferedData.clear();
      } else {
        throw new RuntimeException("Invalid fliush " + currentState);
      }
    }

    /*
    if (!bufferedSourceData.isEmpty()) {
      // flush buffered data
      if (currentState.equals(CurrentState.OFFLOADED)) {

        // Write to offloaded task
        final String edgeId = sourceVertexDataFetchers.get(0).getEdgeId();
        final Serializer serializer = serializerManager.getSerializer(edgeId);
        bufferedSourceData.forEach(e ->
          offloadingManager.offloadSourceData(taskId, edgeId, e, serializer));
        bufferedSourceData.clear();

      } else if (currentState.equals(CurrentState.RUNNING)) {

        final String edgeId = sourceVertexDataFetchers.get(0).getEdgeId();

        bufferedSourceData.forEach(e -> handleInternalData(
          edgeToDataFetcherMap.get(edgeId), e));
        bufferedSourceData.clear();
      } else {
        throw new RuntimeException("Invalid fliush " + currentState);
      }
    }
    */
  }

  @Override
  public void handleData(final String edgeId,
                         final TaskHandlingEvent taskHandlingEvent) {
    if (taskHandlingEvent.isOffloadingMessage()) {
      // control message for prepareOffloading
      final TaskOffloadingEvent event = (TaskOffloadingEvent) taskHandlingEvent;
      final TaskOffloadingEvent.ControlType type = event.getType();

      switch (type) {
        case SEND_TO_OFFLOADING_WORKER: {
          // prepareOffloading task
          checkpoint(false);
          // store watermark  manager
          // final byte[] bytes = FSTSingleton.getInstance().asByteArray(taskWatermarkManager);
          final OutputStream os = stateStore.getOutputStream(taskId + "-taskWatermarkManager");
          taskWatermarkManager.encode(os);
          try {
            os.close();
          } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
          // stateStore.put(taskId + "-taskWatermarkManager", bytes);
          offloadingManager.offloading(taskId);
          currentState = CurrentState.OFFLOAD_PENDING;
          break;
        }
        case OFFLOAD_DONE: {
          LOG.info("Offlodaing done {}", taskId);
          currentState = CurrentState.OFFLOADED;
          flushBuffer();
          LOG.info("End of flush buffer {}", taskId);
          break;
        }
        case DEOFFLOADING: {
          LOG.info("Deoffloading {}", taskId);
          offloadingManager.deoffloading(taskId);
          currentState = CurrentState.DEOFFLOAD_PENDING;
          break;
        }
        case DEOFFLOADING_DONE: {
          // reload states
          LOG.info("Deoffloading done of {}", taskId);
          restore();
          currentState = CurrentState.RUNNING;

          flushBuffer();
          break;
        }
        default:
          throw new RuntimeException("Invalid prepareOffloading control type " + type);
      }
    } else {
      if (taskHandlingEvent instanceof TaskOffloadedDataOutputEvent) {
        // This is the output of the offloaded task
        final TaskOffloadedDataOutputEvent output = (TaskOffloadedDataOutputEvent) taskHandlingEvent;
        if (output.getDstIds().size() > 1) {
          pipeManagerWorker.broadcast(output.getTaskId(),
            output.getEdgeId(),
            output.getDstIds(),
            output.getDataByteBuf());
        } else {
          pipeManagerWorker.writeData(output.getTaskId(),
            output.getEdgeId(),
            output.getDstIds().get(0),
            output.getDataByteBuf());
        }
      } else if (taskHandlingEvent instanceof TaskHandlingDataEvent) {
        // input
        switch (currentState) {
          case OFFLOADED: {
            // We should redirect the data to remote if it is offloaded
            if (!bufferedData.isEmpty()) {
              throw new RuntimeException("buffer should be empty");
            }
            offloadingManager.offloadIntermediateData(taskId, taskHandlingEvent);
            break;
          }
          case DEOFFLOAD_PENDING:
          case OFFLOAD_PENDING:
            bufferedData.add(taskHandlingEvent);
            break;
          case WAIT_WORKER:
          case RUNNING: {
            // Decoding
            if (!bufferedData.isEmpty()) {
              throw new RuntimeException("buffer should be empty");
            }

            final Object data = taskHandlingEvent.getData();
            handleInternalData(edgeToDataFetcherMap.get(edgeId), data);
            break;
          }
          default:
            throw new RuntimeException("Invalid state " + currentState);
        }
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
      final Optional<Watermark> watermark =
        taskWatermarkManager.updateWatermark(dataFetcher.getEdgeId(), d.getIndex(), d.getWatermark().getTimestamp());

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
        switch (currentState) {
          case RUNNING: {
            handleInternalData(dataFetcher, event);
            processed = true;
            break;
          }
          default:
            throw new RuntimeException("Invalid state " + currentState);
        }
          /*
          case OFFLOADED: {
            // We should redirect the data to remote if it is offloaded
            if (!bufferedSourceData.isEmpty()) {
              throw new RuntimeException("buffer should be empty");
            }
            final Serializer serializer = serializerManager.getSerializer(dataFetcher.getEdgeId());
            // LOG.info("Offloading source for task {}", taskId);
            offloadingManager.offloadSourceData(
              taskId,
              dataFetcher.getEdgeId(),
              event,
              serializer);
            processed = true;
            break;
          }
          case DEOFFLOAD_PENDING:
          case OFFLOAD_PENDING:
            bufferedSourceData.add(event);
            break;
          case WAIT_WORKER:
          case RUNNING: {
            // Decoding
            if (!bufferedSourceData.isEmpty()) {
              throw new RuntimeException("buffer should be empty");
            }
            handleInternalData(dataFetcher, event);
            processed = true;
            break;
          }
          default:
            throw new RuntimeException("Invalid state " + currentState);
        }
        */
        //executorMetrics.increaseInputCounter(stageId);
      }
    }
    return processed;
  }


  // For prepareOffloading!
  private void handleOffloadingEvent(final Triple<List<String>, String, Object> triple) {
    //LOG.info("Result handle {} / {} / {}", triple.first, triple.second, triple.third);

    final Object elem = triple.getRight();

    for (final String nextOpId : triple.getLeft()) {
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
      statefulTransform.restore();
    }
  }

  @Override
  public boolean checkpoint(final boolean checkpointSource) {
    boolean hasChekpoint = false;

    // final byte[] bytes = FSTSingleton.getInstance().asByteArray(taskWatermarkManager);
    // stateStore.put(taskId + "-taskWatermarkManager", bytes);

    final OutputStream os = stateStore.getOutputStream(taskId + "-taskWatermarkManager");
    try {
      taskWatermarkManager.encode(os);
      os.close();
    } catch (IOException e) {
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
      statefulTransform.checkpoint();
    }

    return true;
  }


  @Override
  public String toString() {
    return taskId;
  }
}
