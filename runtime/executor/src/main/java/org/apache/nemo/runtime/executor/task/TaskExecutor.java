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
import io.netty.buffer.*;
import org.apache.nemo.common.*;
import org.apache.nemo.compiler.frontend.beam.transform.StatelessOffloadingTransform;
import org.apache.nemo.offloading.client.ServerlessExecutorProvider;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.Edge;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.vertex.*;
import org.apache.nemo.common.ir.vertex.transform.AggregateMetricTransform;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.offloading.client.ServerlessExecutorService;
import org.apache.nemo.offloading.common.Constants;
import org.apache.nemo.runtime.executor.data.SerializerManager;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.common.punctuation.Finishmark;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.runtime.common.state.TaskState;
import org.apache.nemo.runtime.executor.MetricMessageSender;
import org.apache.nemo.runtime.executor.TaskStateManager;
import org.apache.nemo.runtime.executor.TransformContextImpl;
import org.apache.nemo.runtime.executor.data.BroadcastManagerWorker;
import org.apache.nemo.common.Serializer;
import org.apache.nemo.runtime.executor.datatransfer.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nemo.runtime.executor.datatransfer.DynOptDataOutputCollector;
import org.apache.nemo.runtime.executor.datatransfer.OperatorVertexOutputCollector;
import org.apache.nemo.runtime.executor.datatransfer.IntermediateDataIOFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Executes a task.
 * Should be accessed by a single thread.
 */
@NotThreadSafe
public final class TaskExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(TaskExecutor.class.getName());

  // Essential information
  private boolean isExecuted;
  private final String taskId;
  private final TaskStateManager taskStateManager;
  private final List<DataFetcher> dataFetchers;
  private final BroadcastManagerWorker broadcastManagerWorker;
  private final List<VertexHarness> sortedHarnesses;

  // Metrics information
  private long boundedSourceReadTime = 0;
  private long serializedReadBytes = 0;
  private long encodedReadBytes = 0;
  private final MetricMessageSender metricMessageSender;

  // Dynamic optimization
  private String idOfVertexPutOnHold;

  private final PersistentConnectionToMasterMap persistentConnectionToMasterMap;

  private final SerializerManager serializerManager;



  private final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag;

  // Variables for offloading - start
  private long currProcessedEvents = 0;
  private long prevProcessedEvents = 0;
  private Object lock = new Object();
  private long elapsedTimeForProcessedEvents = 1000;
  private long prevProcessedTime = System.currentTimeMillis();
  private final ServerlessExecutorProvider serverlessExecutorProvider;
  private final InputFluctuationDetector detector;
  private final Map<IRVertex, Serializer> srcVertexSerializerMap;
  private volatile boolean offloading = false;
  private transient ServerlessExecutorService serverlessExecutorService;
  private ByteBuf inputBuffer;
  private int serializedCnt = 0;
  private ByteBufOutputStream bos;
  private final Map<String, OutputCollector> vertexIdAndOutputCollectorMap;
  private final Queue<Boolean> offloadingRequestQueue = new LinkedBlockingQueue<>();
  private long prevFlushTime = System.currentTimeMillis();
  // Variables for offloading - end

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
  public TaskExecutor(final Task task,
                      final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
                      final TaskStateManager taskStateManager,
                      final IntermediateDataIOFactory intermediateDataIOFactory,
                      final BroadcastManagerWorker broadcastManagerWorker,
                      final MetricMessageSender metricMessageSender,
                      final PersistentConnectionToMasterMap persistentConnectionToMasterMap,
                      final SerializerManager serializerManager,
                      final ServerlessExecutorProvider serverlessExecutorProvider,
                      final InputFluctuationDetector detector) {
    // Essential information
    this.isExecuted = false;
    this.irVertexDag = irVertexDag;
    this.taskId = task.getTaskId();
    this.srcVertexSerializerMap = new HashMap<>();
    this.taskStateManager = taskStateManager;
    this.broadcastManagerWorker = broadcastManagerWorker;
    this.vertexIdAndOutputCollectorMap = new HashMap<>();
    this.detector = detector;

    this.serverlessExecutorProvider = serverlessExecutorProvider;

    this.serializerManager = serializerManager;

    // Metric sender
    this.metricMessageSender = metricMessageSender;

    // Dynamic optimization
    // Assigning null is very bad, but we are keeping this for now
    this.idOfVertexPutOnHold = null;

    this.persistentConnectionToMasterMap = persistentConnectionToMasterMap;

    // Prepare data structures
    final Pair<List<DataFetcher>, List<VertexHarness>> pair = prepare(task, irVertexDag, intermediateDataIOFactory);
    this.dataFetchers = pair.left();
    this.sortedHarnesses = pair.right();

    //offloadingRequestQueue.add(true);
  }

  private boolean isOperatorFluctuate() {
    return false;
  }

  public synchronized void startOffloading(final long baseTime) {
    LOG.info("Start offloading!");
    if (detector.isInputFluctuation(baseTime)) {
      LOG.info("Input fluctuate!!");
      // do offloading();
      offloadingRequestQueue.add(true);

    } else {
      LOG.info("Operator bursty!!");
    }
  }

  public synchronized void endOffloading() {
    LOG.info("End offloading!");
    // Do sth for offloading end
    offloadingRequestQueue.add(false);
    detector.clear();
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
  private Pair<List<DataFetcher>, List<VertexHarness>> prepare(
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
      if (irVertexDag.getOutgoingEdgesOf(childVertex.getId()).size() == 0) {
        childVertex.isSink = true;
      }

      final List<Edge> edges = TaskExecutorUtil.getAllIncomingEdges(task, irVertexDag, childVertex);
      for (int edgeIndex = 0; edgeIndex < edges.size(); edgeIndex++) {
        final Edge edge = edges.get(edgeIndex);
        edgeIndexMap.putIfAbsent(edge, edgeIndex);
      }
    });

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
              new OperatorWatermarkCollector((OperatorVertex) childVertex)));
        } else {
          operatorWatermarkManagerMap.putIfAbsent(childVertex,
            new MultiInputWatermarkManager(edges.size(),
              new OperatorWatermarkCollector((OperatorVertex) childVertex)));
        }
      }
    });

    // Create a harness for each vertex
    final List<DataFetcher> dataFetcherList = new ArrayList<>();
    final Map<String, VertexHarness> vertexIdToHarness = new HashMap<>();

    reverseTopologicallySorted.forEach(irVertex -> {
      final Optional<Readable> sourceReader = TaskExecutorUtil.getSourceVertexReader(irVertex, task.getIrVertexIdToReadable());
      if (sourceReader.isPresent() != irVertex instanceof SourceVertex) {
        throw new IllegalStateException(irVertex.toString());
      }

      // Additional outputs
      final Map<String, List<NextIntraTaskOperatorInfo>> internalAdditionalOutputMap =
        TaskExecutorUtil.getInternalAdditionalOutputMap(irVertex, irVertexDag, edgeIndexMap, operatorWatermarkManagerMap);
      final Map<String, List<OutputWriter>> externalAdditionalOutputMap =
        TaskExecutorUtil.getExternalAdditionalOutputMap(irVertex, task.getTaskOutgoingEdges(), intermediateDataIOFactory, taskId);

      // Main outputs
      final List<NextIntraTaskOperatorInfo> internalMainOutputs =
        TaskExecutorUtil. getInternalMainOutputs(irVertex, irVertexDag, edgeIndexMap, operatorWatermarkManagerMap);
      final List<OutputWriter> externalMainOutputs =
        TaskExecutorUtil.getExternalMainOutputs(irVertex, task.getTaskOutgoingEdges(), intermediateDataIOFactory, taskId);

      OutputCollector outputCollector;

      if (irVertex instanceof OperatorVertex
        && ((OperatorVertex) irVertex).getTransform() instanceof AggregateMetricTransform) {
        outputCollector = new DynOptDataOutputCollector(
          irVertex, persistentConnectionToMasterMap, this);
      } else {
        outputCollector = new OperatorVertexOutputCollector(
          irVertex, internalMainOutputs, internalAdditionalOutputMap,
          externalMainOutputs, externalAdditionalOutputMap);

        vertexIdAndOutputCollectorMap.put(irVertex.getId(), outputCollector);
      }

      // Create VERTEX HARNESS
      final VertexHarness vertexHarness = new VertexHarness(
        irVertex, outputCollector, new TransformContextImpl(
          broadcastManagerWorker, irVertex, serverlessExecutorProvider),
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
        dataFetcherList.add(new SourceVertexDataFetcher(
          (SourceVertex) irVertex,
          edge,
          sourceReader.get(),
          outputCollector));
      }

      // Parent-task read
      // TODO #285: Cache broadcasted data
      task.getTaskIncomingEdges()
        .stream()
        .filter(inEdge -> inEdge.getDstIRVertex().getId().equals(irVertex.getId())) // edge to this vertex
        .map(incomingEdge -> {

          return Pair.of(incomingEdge, intermediateDataIOFactory
            .createReader(taskIndex, incomingEdge.getSrcIRVertex(), incomingEdge));
        })
        .forEach(pair -> {
          if (irVertex instanceof OperatorVertex) {

            final StageEdge edge = pair.left();
            final int edgeIndex = edgeIndexMap.get(edge);
            final InputWatermarkManager watermarkManager = operatorWatermarkManagerMap.get(irVertex);
            final InputReader parentTaskReader = pair.right();
            final OutputCollector dataFetcherOutputCollector =
              new DataFetcherOutputCollector((OperatorVertex) irVertex, edgeIndex, watermarkManager);

            if (parentTaskReader instanceof PipeInputReader) {
              dataFetcherList.add(
                new MultiThreadParentTaskDataFetcher(
                  parentTaskReader.getSrcIrVertex(),
                  edge,
                  parentTaskReader,
                  dataFetcherOutputCollector));
            } else {
              dataFetcherList.add(
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

    return Pair.of(dataFetcherList, sortedHarnessList);
  }

  /**
   * Process a data element down the DAG dependency.
   */
  private void processElement(final OutputCollector outputCollector, final Object dataElement) {
    outputCollector.emit(dataElement);
  }

  private void processWatermark(final OutputCollector outputCollector,
                                final Watermark watermark) {
    outputCollector.emitWatermark(watermark);
  }

  /**
   * Execute a task, while handling unrecoverable errors and exceptions.
   */
  public void execute() {
    try {
      doExecute();
    } catch (Throwable throwable) {
      // ANY uncaught throwable is reported to the master
      taskStateManager.onTaskStateChanged(TaskState.State.FAILED, Optional.empty(), Optional.empty());
      LOG.error(ExceptionUtils.getStackTrace(throwable));
    }
  }

  /**
   * The task is executed in the following two phases.
   * - Phase 1: Consume task-external input data
   * - Phase 2: Finalize task-internal states and data elements
   */
  private void doExecute() {
    // Housekeeping stuff
    if (isExecuted) {
      throw new RuntimeException("Task {" + taskId + "} execution called again");
    }
    LOG.info("{} started", taskId);
    taskStateManager.onTaskStateChanged(TaskState.State.EXECUTING, Optional.empty(), Optional.empty());

    // Phase 1: Consume task-external input data.
    if (!handleDataFetchers(dataFetchers)) {
      return;
    }

    metricMessageSender.send("TaskMetric", taskId,
      "boundedSourceReadTime", SerializationUtils.serialize(boundedSourceReadTime));
    metricMessageSender.send("TaskMetric", taskId,
      "serializedReadBytes", SerializationUtils.serialize(serializedReadBytes));
    metricMessageSender.send("TaskMetric", taskId,
      "encodedReadBytes", SerializationUtils.serialize(encodedReadBytes));

    // Phase 2: Finalize task-internal states and elements
    for (final VertexHarness vertexHarness : sortedHarnesses) {
      finalizeVertex(vertexHarness);
    }

    if (idOfVertexPutOnHold == null) {
      taskStateManager.onTaskStateChanged(TaskState.State.COMPLETE, Optional.empty(), Optional.empty());
      LOG.info("{} completed", taskId);
    } else {
      taskStateManager.onTaskStateChanged(TaskState.State.ON_HOLD,
        Optional.of(idOfVertexPutOnHold),
        Optional.empty());
      LOG.info("{} on hold", taskId);
    }
  }

  private void finalizeVertex(final VertexHarness vertexHarness) {
    closeTransform(vertexHarness);
    finalizeOutputWriters(vertexHarness);
  }

  private void flushToServerless() {
    LOG.info("Flush to serverless: {}", serializedCnt);
    final CompositeByteBuf compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeBuffer(2);
    final ByteBuf lengthBuf = PooledByteBufAllocator.DEFAULT.buffer(4);
    lengthBuf.writeInt(serializedCnt);
    compositeByteBuf.addComponents(true, lengthBuf, inputBuffer);
    // execute
    serverlessExecutorService.execute(compositeByteBuf);
  }

  private void sendToServerless(final Object event,
                                final DataFetcher dataFetcher) {
    final String id = dataFetcher.getDataSource().getId();
    final Serializer serializer = serializerManager.getSerializer(dataFetcher.edge.getId());

    //LOG.info("Send from {}/{} to serverless, cnt: {}", id, dataFetcher.edge.getId(),
    // serializedCnt);

    try {
      bos.writeUTF(id);
      bos.writeUTF(dataFetcher.edge.getId());
      serializer.getEncoderFactory().create(bos).encode(event);
      serializedCnt += 1;

      if (inputBuffer.readableBytes() > Constants.FLUSH_BYTES ||
        System.currentTimeMillis() - prevFlushTime > 1000) {
      //if (serializedCnt > 10) {

        // flush
        flushToServerless();
        prevFlushTime = System.currentTimeMillis();

        // reset
        inputBuffer = PooledByteBufAllocator.DEFAULT.buffer();
        bos = new ByteBufOutputStream(inputBuffer);
        serializedCnt = 0;
      }
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  /**
   * Process an event generated from the dataFetcher.
   * If the event is an instance of Finishmark, we remove the dataFetcher from the current list.
   * @param event event
   * @param dataFetcher current data fetcher
   */
  private void onEventFromDataFetcher(final Object event,
                                      final DataFetcher dataFetcher) {

    if (!offloadingRequestQueue.isEmpty()) {
      offloading = offloadingRequestQueue.poll();
      if (offloading) {
        LOG.info("Initialize offloading");
        // start offloading
        prevFlushTime = System.currentTimeMillis();
        inputBuffer = PooledByteBufAllocator.DEFAULT.buffer();
        bos = new ByteBufOutputStream(inputBuffer);
        serverlessExecutorService = serverlessExecutorProvider.
          newCachedPool(new StatelessOffloadingTransform(irVertexDag),
            new StatelessOffloadingSerializer(serializerManager.runtimeEdgeIdToSerializer),
            new StatelessOffloadingEventHandler(vertexIdAndOutputCollectorMap));
      } else {
        // stop offloading
        if (inputBuffer.readableBytes() > 0) {
          // TODO: send remaining data to serverless
          flushToServerless();
        }

        // reset
        serverlessExecutorService.shutdown();
        serializedCnt = 0;
        try {
          bos.close();
        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
    }

    if (event instanceof Finishmark) {
      // We've consumed all the data from this data fetcher.
      if (dataFetcher instanceof SourceVertexDataFetcher) {
        boundedSourceReadTime += ((SourceVertexDataFetcher) dataFetcher).getBoundedSourceReadTime();
      } else if (dataFetcher instanceof ParentTaskDataFetcher) {
        serializedReadBytes += ((ParentTaskDataFetcher) dataFetcher).getSerializedBytes();
        encodedReadBytes += ((ParentTaskDataFetcher) dataFetcher).getEncodedBytes();
      } else if (dataFetcher instanceof MultiThreadParentTaskDataFetcher) {
        serializedReadBytes += ((MultiThreadParentTaskDataFetcher) dataFetcher).getSerializedBytes();
        encodedReadBytes += ((MultiThreadParentTaskDataFetcher) dataFetcher).getEncodedBytes();
      }
    } else if (event instanceof Watermark) {
      // Watermark
      if (offloading) {
        sendToServerless(event, dataFetcher);
      } else {
        processWatermark(dataFetcher.getOutputCollector(), (Watermark) event);
      }
    } else {
      // Process data element
      if (offloading) {
        sendToServerless(event, dataFetcher);
      } else {
        processElement(dataFetcher.getOutputCollector(), event);
      }
    }
  }

  /**
   * Check if it is time to poll pending fetchers' data.
   * @param pollingPeriod polling period
   * @param currentTime current time
   * @param prevTime prev time
   */
  private boolean isPollingTime(final long pollingPeriod,
                                final long currentTime,
                                final long prevTime) {
    return (currentTime - prevTime) >= pollingPeriod;
  }

  /**
   * This retrieves data from data fetchers and process them.
   * It maintains two lists:
   *  -- availableFetchers: maintain data fetchers that currently have data elements to retreive
   *  -- pendingFetchers: maintain data fetchers that currently do not have available elements.
   *     This can become available in the future, and therefore we check the pending fetchers every pollingInterval.
   *
   *  If a data fetcher finishes, we remove it from the two lists.
   *  If a data fetcher has no available element, we move the data fetcher to pendingFetchers
   *  If a pending data fetcher has element, we move it to availableFetchers
   *  If there are no available fetchers but pending fetchers, sleep for pollingPeriod
   *  and retry fetching data from the pendingFetchers.
   *
   * @param fetchers to handle.
   * @return false if IOException.
   */
  private boolean handleDataFetchers(final List<DataFetcher> fetchers) {
    final List<DataFetcher> availableFetchers = new LinkedList<>(fetchers);
    final List<DataFetcher> pendingFetchers = new LinkedList<>();

    // Polling interval.
    final long pollingInterval = 100; // ms

    // Previous polling time
    long prevPollingTime = System.currentTimeMillis();
    long processingTime = 0;
    long fetchTime = 0;


    // empty means we've consumed all task-external input data
    while (!availableFetchers.isEmpty() || !pendingFetchers.isEmpty()) {


      // We first fetch data from available data fetchers
      final Iterator<DataFetcher> availableIterator = availableFetchers.iterator();

      while (availableIterator.hasNext()) {

        final long currTime = System.currentTimeMillis();
        if (currTime - prevProcessedTime >= elapsedTimeForProcessedEvents) {
          synchronized (lock) {
            LOG.info("# of processed events (during {} ms) in TaskExecutor {}: {}",
              (currTime - prevProcessedTime), taskId, currProcessedEvents);
            prevProcessedEvents = currProcessedEvents;
            prevProcessedTime = System.currentTimeMillis();
            currProcessedEvents = 0;
            detector.collect(prevProcessedTime, prevProcessedEvents);
          }
        }

        final DataFetcher dataFetcher = availableIterator.next();
        try {
          //final long a = System.currentTimeMillis();
          final Object element = dataFetcher.fetchDataElement();

          //fetchTime += (System.currentTimeMillis() - a);

          //final long b = System.currentTimeMillis();
          onEventFromDataFetcher(element, dataFetcher);
          currProcessedEvents += 1;
          //processingTime += (System.currentTimeMillis() - b);

          if (element instanceof Finishmark) {
            availableIterator.remove();
          }
        } catch (final NoSuchElementException e) {
          // No element in current data fetcher, fetch data from next fetcher
          // move current data fetcher to pending.
          availableIterator.remove();
          pendingFetchers.add(dataFetcher);
        } catch (final IOException e) {
          // IOException means that this task should be retried.
          taskStateManager.onTaskStateChanged(TaskState.State.SHOULD_RETRY,
            Optional.empty(), Optional.of(TaskState.RecoverableTaskFailureCause.INPUT_READ_FAILURE));
          LOG.error("{} Execution Failed (Recoverable: input read failure)! Exception: {}", taskId, e);
          return false;
        }
      }

      final Iterator<DataFetcher> pendingIterator = pendingFetchers.iterator();
      final long currentTime = System.currentTimeMillis();


      if (isPollingTime(pollingInterval, currentTime, prevPollingTime)) {
        // We check pending data every polling interval
        prevPollingTime = currentTime;

        while (pendingIterator.hasNext()) {

          final long currTime = System.currentTimeMillis();
          if (currTime - prevProcessedTime >= elapsedTimeForProcessedEvents) {
            LOG.info("# of processed events (during {} ms) in TaskExecutor {}: {}",
              (currTime - prevProcessedTime), taskId, currProcessedEvents);
            prevProcessedEvents = currProcessedEvents;
            currProcessedEvents = 0;
            prevProcessedTime = System.currentTimeMillis();
            detector.collect(prevProcessedTime, prevProcessedEvents);
          }

          final DataFetcher dataFetcher = pendingIterator.next();
          try {
            //final long a = System.currentTimeMillis();
            final Object element = dataFetcher.fetchDataElement();
            //fetchTime += (System.currentTimeMillis() - a);

            //final long b = System.currentTimeMillis();
            onEventFromDataFetcher(element, dataFetcher);
            currProcessedEvents += 1;
           // processingTime += (System.currentTimeMillis() - b);

            // We processed data. This means the data fetcher is now available.
            // Add current data fetcher to available
            pendingIterator.remove();
            if (!(element instanceof Finishmark)) {
              availableFetchers.add(dataFetcher);
            }

          } catch (final NoSuchElementException e) {
            // The current data fetcher is still pending.. try next data fetcher
          } catch (final IOException e) {
            // IOException means that this task should be retried.
            taskStateManager.onTaskStateChanged(TaskState.State.SHOULD_RETRY,
              Optional.empty(), Optional.of(TaskState.RecoverableTaskFailureCause.INPUT_READ_FAILURE));
            LOG.error("{} Execution Failed (Recoverable: input read failure)! Exception: {}", taskId, e);
            return false;
          }
        }
      }

      // If there are no available fetchers,
      // Sleep and retry fetching element from pending fetchers every polling interval
      if (availableFetchers.isEmpty() && !pendingFetchers.isEmpty()) {
        try {
          Thread.sleep(pollingInterval);
        } catch (InterruptedException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
    }

    // Close all data fetchers
    fetchers.forEach(fetcher -> {
      try {
        fetcher.close();
      } catch (final Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    });

    return true;
  }


  private List<InputReader> getParentTaskReaders(final int taskIndex,
                                                 final List<StageEdge> inEdgesFromParentTasks,
                                                 final IntermediateDataIOFactory intermediateDataIOFactory) {
    return inEdgesFromParentTasks
      .stream()
      .map(inEdgeForThisVertex -> intermediateDataIOFactory
        .createReader(taskIndex, inEdgeForThisVertex.getSrcIRVertex(), inEdgeForThisVertex))
      .collect(Collectors.toList());
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
    idOfVertexPutOnHold = irVertex.getId();
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


}
