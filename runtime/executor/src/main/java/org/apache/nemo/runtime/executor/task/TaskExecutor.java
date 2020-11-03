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
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.Edge;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.edge.executionproperty.AdditionalOutputTagProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.ir.vertex.transform.MessageAggregatorTransform;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.punctuation.Finishmark;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.runtime.common.plan.RuntimeEdge;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.common.state.TaskState;
import org.apache.nemo.runtime.executor.MetricMessageSender;
import org.apache.nemo.runtime.executor.TaskStateManager;
import org.apache.nemo.runtime.executor.TransformContextImpl;
import org.apache.nemo.runtime.executor.data.BroadcastManagerWorker;
import org.apache.nemo.runtime.executor.datatransfer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Executes a task.
 * Should be accessed by a single thread.
 */
@NotThreadSafe
public final class TaskExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(TaskExecutor.class.getName());
  private static final String TASK_METRIC_ID = "TaskMetric";

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
  private long numOfProcessedEvents = 0;
  private long timeSinceLastExecution;
  private final MetricMessageSender metricMessageSender;

  // Dynamic optimization
  private String idOfVertexPutOnHold;

  private final PersistentConnectionToMasterMap persistentConnectionToMasterMap;

  /**
   * Constructor.
   *
   * @param task                            Task with information needed during execution.
   * @param irVertexDag                     A DAG of vertices.
   * @param taskStateManager                State manager for this Task.
   * @param intermediateDataIOFactory       For reading from/writing to data to other tasks.
   * @param broadcastManagerWorker          For broadcasts.
   * @param metricMessageSender             For sending metric with execution stats to the master.
   * @param persistentConnectionToMasterMap For sending messages to the master.
   */
  public TaskExecutor(final Task task,
                      final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
                      final TaskStateManager taskStateManager,
                      final IntermediateDataIOFactory intermediateDataIOFactory,
                      final BroadcastManagerWorker broadcastManagerWorker,
                      final MetricMessageSender metricMessageSender,
                      final PersistentConnectionToMasterMap persistentConnectionToMasterMap) {
    // Essential information
    this.isExecuted = false;
    this.taskId = task.getTaskId();
    this.taskStateManager = taskStateManager;
    this.broadcastManagerWorker = broadcastManagerWorker;

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

    this.timeSinceLastExecution = System.currentTimeMillis();
  }

  // Get all of the intra-task edges + inter-task edges
  private List<Edge> getAllIncomingEdges(
    final Task task,
    final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
    final IRVertex childVertex) {
    final List<Edge> edges = new ArrayList<>();
    edges.addAll(irVertexDag.getIncomingEdgesOf(childVertex));
    final List<StageEdge> taskEdges = task.getTaskIncomingEdges().stream()
      .filter(edge -> edge.getDstIRVertex().getId().equals(childVertex.getId()))
      .collect(Collectors.toList());
    edges.addAll(taskEdges);
    return edges;
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
   * @param task                      task.
   * @param irVertexDag               dag.
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
      final List<Edge> edges = getAllIncomingEdges(task, irVertexDag, childVertex);
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
        final List<Edge> edges = getAllIncomingEdges(task, irVertexDag, childVertex);
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
      final Optional<Readable> sourceReader = getSourceVertexReader(irVertex, task.getIrVertexIdToReadable());
      if (sourceReader.isPresent() != irVertex instanceof SourceVertex) {
        throw new IllegalStateException(irVertex.toString());
      }

      // Additional outputs
      final Map<String, List<NextIntraTaskOperatorInfo>> internalAdditionalOutputMap =
        getInternalOutputMap(irVertex, irVertexDag, edgeIndexMap, operatorWatermarkManagerMap);
      final Map<String, List<OutputWriter>> externalAdditionalOutputMap =
        getExternalAdditionalOutputMap(irVertex, task.getTaskOutgoingEdges(), intermediateDataIOFactory);

      // Main outputs
      final List<NextIntraTaskOperatorInfo> internalMainOutputs;
      if (internalAdditionalOutputMap.containsKey(AdditionalOutputTagProperty.getMainOutputTag())) {
        internalMainOutputs = internalAdditionalOutputMap.remove(AdditionalOutputTagProperty.getMainOutputTag());
      } else {
        internalMainOutputs = new ArrayList<>();
      }
      final List<OutputWriter> externalMainOutputs =
        getExternalMainOutputs(irVertex, task.getTaskOutgoingEdges(), intermediateDataIOFactory);

      final OutputCollector outputCollector;

      if (irVertex instanceof OperatorVertex
        && ((OperatorVertex) irVertex).getTransform() instanceof MessageAggregatorTransform) {
        outputCollector = new RunTimeMessageOutputCollector(
          taskId, irVertex, persistentConnectionToMasterMap, this);
      } else {
        outputCollector = new OperatorVertexOutputCollector(
          irVertex, internalMainOutputs, internalAdditionalOutputMap,
          externalMainOutputs, externalAdditionalOutputMap);
      }

      // Create VERTEX HARNESS
      final VertexHarness vertexHarness = new VertexHarness(
        irVertex, outputCollector, new TransformContextImpl(broadcastManagerWorker),
        externalMainOutputs, externalAdditionalOutputMap);

      prepareTransform(vertexHarness);
      vertexIdToHarness.put(irVertex.getId(), vertexHarness);

      // Prepare data READ
      // Source read
      if (irVertex instanceof SourceVertex) {
        // Source vertex read
        dataFetcherList.add(new SourceVertexDataFetcher(
          (SourceVertex) irVertex,
          sourceReader.get(),
          outputCollector));
      }

      // Parent-task read
      // TODO #285: Cache broadcasted data
      task.getTaskIncomingEdges()
        .stream()
        .filter(inEdge -> inEdge.getDstIRVertex().getId().equals(irVertex.getId())) // edge to this vertex
        .map(incomingEdge ->
          Pair.of(incomingEdge, intermediateDataIOFactory
            .createReader(task.getTaskId(), incomingEdge.getSrcIRVertex(), incomingEdge)))
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
                  parentTaskReader,
                  dataFetcherOutputCollector));
            } else {
              dataFetcherList.add(
                new ParentTaskDataFetcher(
                  parentTaskReader.getSrcIrVertex(),
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
    final long executionStartTime = System.currentTimeMillis();
    metricMessageSender.send(TASK_METRIC_ID, taskId, "schedulingOverhead",
      SerializationUtils.serialize(executionStartTime - timeSinceLastExecution));

    // Phase 1: Consume task-external input data.
    if (!handleDataFetchers(dataFetchers)) {
      return;
    }

    sendMetrics();

    // Phase 2: Finalize task-internal states and elements
    for (final VertexHarness vertexHarness : sortedHarnesses) {
      finalizeVertex(vertexHarness);
    }

    this.timeSinceLastExecution = System.currentTimeMillis();
    metricMessageSender.send(TASK_METRIC_ID, taskId, "taskDuration",
      SerializationUtils.serialize(timeSinceLastExecution - executionStartTime));
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

  /**
   * Send data-processing metrics.
   */
  public void sendMetrics() {
    metricMessageSender.send(TASK_METRIC_ID, taskId, "boundedSourceReadTime",
      SerializationUtils.serialize(boundedSourceReadTime));
    metricMessageSender.send(TASK_METRIC_ID, taskId, "serializedReadBytes",
      SerializationUtils.serialize(serializedReadBytes));
    metricMessageSender.send(TASK_METRIC_ID, taskId, "encodedReadBytes",
      SerializationUtils.serialize(encodedReadBytes));
    metricMessageSender.send(TASK_METRIC_ID, taskId, "numOfProcessedElements",
      SerializationUtils.serialize(numOfProcessedEvents));
  }

  /**
   * Finalize the vertex.
   * @param vertexHarness the vertex harness.
   */
  private void finalizeVertex(final VertexHarness vertexHarness) {
    closeTransform(vertexHarness);
    finalizeOutputWriters(vertexHarness);
  }

  /**
   * Process an event generated from the dataFetcher.
   * If the event is an instance of Finishmark, we remove the dataFetcher from the current list.
   *
   * @param event       event
   * @param dataFetcher current data fetcher
   */
  private void onEventFromDataFetcher(final Object event,
                                      final DataFetcher dataFetcher) {
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
      processWatermark(dataFetcher.getOutputCollector(), (Watermark) event);
    } else {
      // Process data element
      processElement(dataFetcher.getOutputCollector(), event);
    }
    this.numOfProcessedEvents++;
  }

  /**
   * Check if it is time to poll pending fetchers' data.
   *
   * @param pollingPeriod polling period
   * @param currentTime   current time
   * @param prevTime      prev time
   */
  private boolean isPollingTime(final long pollingPeriod, final long currentTime, final long prevTime) {
    return (currentTime - prevTime) >= pollingPeriod;
  }

  /**
   * This retrieves data from data fetchers and process them.
   * It maintains two lists:
   * -- availableFetchers: maintain data fetchers that currently have data elements to retreive
   * -- pendingFetchers: maintain data fetchers that currently do not have available elements.
   * This can become available in the future, and therefore we check the pending fetchers every pollingInterval.
   * <p>
   * If a data fetcher finishes, we remove it from the two lists.
   * If a data fetcher has no available element, we move the data fetcher to pendingFetchers
   * If a pending data fetcher has element, we move it to availableFetchers
   * If there are no available fetchers but pending fetchers, sleep for pollingPeriod
   * and retry fetching data from the pendingFetchers.
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

    // empty means we've consumed all task-external input data
    while (!availableFetchers.isEmpty() || !pendingFetchers.isEmpty()) {
      // We first fetch data from available data fetchers
      final Iterator<DataFetcher> availableIterator = availableFetchers.iterator();

      while (availableIterator.hasNext()) {
        final DataFetcher dataFetcher = availableIterator.next();
        try {
          final Object element = dataFetcher.fetchDataElement();
          onEventFromDataFetcher(element, dataFetcher);
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
          final DataFetcher dataFetcher = pendingIterator.next();
          try {
            final Object element = dataFetcher.fetchDataElement();
            onEventFromDataFetcher(element, dataFetcher);

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
          Thread.currentThread().interrupt();
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

  ////////////////////////////////////////////// Helper methods for setting up initial data structures
  private Map<String, List<OutputWriter>> getExternalAdditionalOutputMap(
    final IRVertex irVertex,
    final List<StageEdge> outEdgesToChildrenTasks,
    final IntermediateDataIOFactory intermediateDataIOFactory) {
    // Add all inter-task additional tags to additional output map.
    final Map<String, List<OutputWriter>> map = new HashMap<>();

    outEdgesToChildrenTasks
      .stream()
      .filter(edge -> edge.getSrcIRVertex().getId().equals(irVertex.getId()))
      .filter(edge -> edge.getPropertyValue(AdditionalOutputTagProperty.class).isPresent())
      .map(edge ->
        Pair.of(edge.getPropertyValue(AdditionalOutputTagProperty.class).get(),
          intermediateDataIOFactory.createWriter(taskId, edge)))
      .forEach(pair -> {
        map.putIfAbsent(pair.left(), new ArrayList<>());
        map.get(pair.left()).add(pair.right());
      });

    return map;
  }

  /**
   * Return a map of Internal Outputs associated with their output tag.
   * If an edge has no output tag, its info are added to the mainOutputTag.
   *
   * @param irVertex                    source irVertex
   * @param irVertexDag                 DAG of IRVertex and RuntimeEdge
   * @param edgeIndexMap                Map of edge and index
   * @param operatorWatermarkManagerMap Map of irVertex and InputWatermarkManager
   * @return The map of output tag to the list of next intra-task operator information.
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
        return Pair.of(outputTag, new NextIntraTaskOperatorInfo(index, nextOperator, inputWatermarkManager));
      })
      .forEach(pair -> {
        map.putIfAbsent(pair.left(), new ArrayList<>());
        map.get(pair.left()).add(pair.right());
      });

    return map;
  }

  /**
   * Return inter-task OutputWriters, for single output or output associated with main tag.
   *
   * @param irVertex                  source irVertex
   * @param outEdgesToChildrenTasks   outgoing edges to child tasks
   * @param intermediateDataIOFactory intermediateDataIOFactory
   * @return OutputWriters for main children tasks
   */
  private List<OutputWriter> getExternalMainOutputs(final IRVertex irVertex,
                                                    final List<StageEdge> outEdgesToChildrenTasks,
                                                    final IntermediateDataIOFactory intermediateDataIOFactory) {
    return outEdgesToChildrenTasks
      .stream()
      .filter(edge -> edge.getSrcIRVertex().getId().equals(irVertex.getId()))
      .filter(edge -> !edge.getPropertyValue(AdditionalOutputTagProperty.class).isPresent())
      .map(outEdgeForThisVertex -> intermediateDataIOFactory
        .createWriter(taskId, outEdgeForThisVertex))
      .collect(Collectors.toList());
  }


  private Optional<Readable> getSourceVertexReader(final IRVertex irVertex,
                                                   final Map<String, Readable> irVertexIdToReadable) {
    if (irVertex instanceof SourceVertex) {
      final Readable readable = irVertexIdToReadable.get(irVertex.getId());
      if (readable == null) {
        throw new IllegalStateException(irVertex.toString());
      }
      return Optional.of(readable);
    } else {
      return Optional.empty();
    }
  }

  private List<InputReader> getParentTaskReaders(final String dstTaskId,
                                                 final List<StageEdge> inEdgesFromParentTasks,
                                                 final IntermediateDataIOFactory intermediateDataIOFactory) {
    return inEdgesFromParentTasks
      .stream()
      .map(inEdgeForThisVertex -> intermediateDataIOFactory
        .createReader(dstTaskId, inEdgeForThisVertex.getSrcIRVertex(), inEdgeForThisVertex))
      .collect(Collectors.toList());
  }

  ////////////////////////////////////////////// Transform-specific helper methods

  private void prepareTransform(final VertexHarness vertexHarness) {
    final IRVertex irVertex = vertexHarness.getIRVertex();
    final Transform transform;
    if (irVertex instanceof OperatorVertex) {
      transform = ((OperatorVertex) irVertex).getTransform();
      transform.prepare(vertexHarness.getContext(), vertexHarness.getOutputCollector());
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
    vertexHarness.getWritersToAdditionalChildrenTasks().values().forEach(outputWriters ->
      outputWriters.forEach(outputWriter -> {
        outputWriter.close();
        final Optional<Long> writtenBytes = outputWriter.getWrittenBytes();
        writtenBytes.ifPresent(writtenBytesList::add);
      })
    );

    long totalWrittenBytes = 0;
    for (final Long writtenBytes : writtenBytesList) {
      totalWrittenBytes += writtenBytes;
    }

    // TODO #236: Decouple metric collection and sending logic
    metricMessageSender.send(TASK_METRIC_ID, taskId, "taskOutputBytes",
      SerializationUtils.serialize(totalWrittenBytes));
  }
}
