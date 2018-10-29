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
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.edge.executionproperty.AdditionalOutputTagProperty;
import org.apache.nemo.common.ir.edge.executionproperty.BroadcastVariableIdProperty;
import org.apache.nemo.common.ir.vertex.*;
import org.apache.nemo.common.ir.vertex.transform.AggregateMetricTransform;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.runtime.common.plan.RuntimeEdge;
import org.apache.nemo.runtime.common.state.TaskState;
import org.apache.nemo.runtime.executor.MetricMessageSender;
import org.apache.nemo.runtime.executor.TaskStateManager;
import org.apache.nemo.runtime.executor.TransformContextImpl;
import org.apache.nemo.runtime.executor.data.BroadcastManagerWorker;
import org.apache.nemo.runtime.executor.datatransfer.*;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nemo.runtime.executor.datatransfer.DynOptDataOutputCollector;
import org.apache.nemo.runtime.executor.datatransfer.OperatorVertexOutputCollector;
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
  private static final int NONE_FINISHED = -1;

  // Essential information
  private boolean isExecuted;
  private final String taskId;
  private final TaskStateManager taskStateManager;
  private final List<DataFetcher> nonBroadcastDataFetchers;
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

  /**
   * Constructor.
   *
   * @param task                   Task with information needed during execution.
   * @param irVertexDag            A DAG of vertices.
   * @param taskStateManager       State manager for this Task.
   * @param intermediateDataIOFactory    For reading from/writing to data to other tasks.
   * @param broadcastManagerWorker For broadcasts.
   * @param metricMessageSender    For sending metric with execution stats to Master.
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
    this.nonBroadcastDataFetchers = pair.left();
    this.sortedHarnesses = pair.right();
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

    // Create a harness for each vertex
    final List<DataFetcher> nonBroadcastDataFetcherList = new ArrayList<>();
    final Map<String, VertexHarness> vertexIdToHarness = new HashMap<>();
    reverseTopologicallySorted.forEach(irVertex -> {
      final Optional<Readable> sourceReader = getSourceVertexReader(irVertex, task.getIrVertexIdToReadable());
      if (sourceReader.isPresent() != irVertex instanceof SourceVertex) {
        throw new IllegalStateException(irVertex.toString());
      }

      // Additional outputs
      final Map<String, List<OperatorVertex>> internalAdditionalOutputMap =
        getInternalAdditionalOutputMap(irVertex, irVertexDag);
      final Map<String, List<OutputWriter>> externalAdditionalOutputMap =
        getExternalAdditionalOutputMap(irVertex, task.getTaskOutgoingEdges(), intermediateDataIOFactory);

      // Main outputs
      final List<OperatorVertex> internalMainOutputs = getInternalMainOutputs(irVertex, irVertexDag);
      final List<OutputWriter> externalMainOutputs =
        getExternalMainOutputs(irVertex, task.getTaskOutgoingEdges(), intermediateDataIOFactory);

      final OutputCollector outputCollector;

      if (irVertex instanceof OperatorVertex
        && ((OperatorVertex) irVertex).getTransform() instanceof AggregateMetricTransform) {
        outputCollector = new DynOptDataOutputCollector(
          irVertex, persistentConnectionToMasterMap, this);
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
        nonBroadcastDataFetcherList.add(new SourceVertexDataFetcher(irVertex, sourceReader.get(), outputCollector));
      }

      // Parent-task read (broadcasts)
      final List<StageEdge> inEdgesForThisVertex = task.getTaskIncomingEdges()
        .stream()
        .filter(inEdge -> inEdge.getDstIRVertex().getId().equals(irVertex.getId()))
        .collect(Collectors.toList());
      final List<StageEdge> broadcastInEdges = inEdgesForThisVertex
        .stream()
        .filter(stageEdge -> stageEdge.getPropertyValue(BroadcastVariableIdProperty.class).isPresent())
        .collect(Collectors.toList());
      final List<InputReader> broadcastReaders =
        getParentTaskReaders(taskIndex, broadcastInEdges, intermediateDataIOFactory);
      if (broadcastInEdges.size() != broadcastReaders.size()) {
        throw new IllegalStateException(broadcastInEdges.toString() + ", " + broadcastReaders.toString());
      }
      for (int i = 0; i < broadcastInEdges.size(); i++) {
        final StageEdge inEdge = broadcastInEdges.get(i);
        broadcastManagerWorker.registerInputReader(
          inEdge.getPropertyValue(BroadcastVariableIdProperty.class)
            .orElseThrow(() -> new IllegalStateException(inEdge.toString())),
          broadcastReaders.get(i));
      }
      // Parent-task read (non-broadcasts)
      final List<StageEdge> nonBroadcastInEdges = new ArrayList<>(inEdgesForThisVertex);
      nonBroadcastInEdges.removeAll(broadcastInEdges);
      final List<InputReader> nonBroadcastReaders =
        getParentTaskReaders(taskIndex, nonBroadcastInEdges, intermediateDataIOFactory);
      nonBroadcastReaders.forEach(parentTaskReader -> nonBroadcastDataFetcherList.add(
        new ParentTaskDataFetcher(parentTaskReader.getSrcIrVertex(), parentTaskReader,
          new DataFetcherOutputCollector((OperatorVertex) irVertex))));
    });

    final List<VertexHarness> sortedHarnessList = irVertexDag.getTopologicalSort()
      .stream()
      .map(vertex -> vertexIdToHarness.get(vertex.getId()))
      .collect(Collectors.toList());

    return Pair.of(nonBroadcastDataFetcherList, sortedHarnessList);
  }

  /**
   * Process a data element down the DAG dependency.
   */
  private void processElement(final OutputCollector outputCollector, final Object dataElement) {
    outputCollector.emit(dataElement);
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
   * - Phase 1: Consume task-external input data (non-broadcasts)
   * - Phase 2: Finalize task-internal states and data elements
   */
  private void doExecute() {
    // Housekeeping stuff
    if (isExecuted) {
      throw new RuntimeException("Task {" + taskId + "} execution called again");
    }
    LOG.info("{} started", taskId);
    taskStateManager.onTaskStateChanged(TaskState.State.EXECUTING, Optional.empty(), Optional.empty());

    // Phase 1: Consume task-external input data. (non-broadcasts)
    if (!handleDataFetchers(nonBroadcastDataFetchers)) {
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

  /**
   * @param fetchers to handle.
   * @return false if IOException.
   */
  private boolean handleDataFetchers(final List<DataFetcher> fetchers) {
    final List<DataFetcher> availableFetchers = new ArrayList<>(fetchers);
    while (!availableFetchers.isEmpty()) { // empty means we've consumed all task-external input data
      // For this looping of available fetchers.
      int finishedFetcherIndex = NONE_FINISHED;
      for (int i = 0; i < availableFetchers.size(); i++) {
        final DataFetcher dataFetcher = availableFetchers.get(i);
        final Object element;
        try {
          element = dataFetcher.fetchDataElement();
        } catch (NoSuchElementException e) {
          // We've consumed all the data from this data fetcher.
          if (dataFetcher instanceof SourceVertexDataFetcher) {
            boundedSourceReadTime += ((SourceVertexDataFetcher) dataFetcher).getBoundedSourceReadTime();
          } else if (dataFetcher instanceof ParentTaskDataFetcher) {
            serializedReadBytes += ((ParentTaskDataFetcher) dataFetcher).getSerializedBytes();
            encodedReadBytes += ((ParentTaskDataFetcher) dataFetcher).getEncodedBytes();
          }
          finishedFetcherIndex = i;
          break;
        } catch (IOException e) {
          // IOException means that this task should be retried.
          taskStateManager.onTaskStateChanged(TaskState.State.SHOULD_RETRY,
            Optional.empty(), Optional.of(TaskState.RecoverableTaskFailureCause.INPUT_READ_FAILURE));
          LOG.error("{} Execution Failed (Recoverable: input read failure)! Exception: {}", taskId, e);
          return false;
        }

        // Successfully fetched an element
        processElement(dataFetcher.getOutputCollector(), element);
      }

      // Remove the finished fetcher from the list
      if (finishedFetcherIndex != NONE_FINISHED) {
        availableFetchers.remove(finishedFetcherIndex);
      }
    }
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

  private Map<String, List<OperatorVertex>> getInternalAdditionalOutputMap(
    final IRVertex irVertex,
    final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag) {
    // Add all intra-task additional tags to additional output map.
    final Map<String, List<OperatorVertex>> map = new HashMap<>();

    irVertexDag.getOutgoingEdgesOf(irVertex.getId())
      .stream()
      .filter(edge -> edge.getPropertyValue(AdditionalOutputTagProperty.class).isPresent())
      .map(edge ->
        Pair.of(edge.getPropertyValue(AdditionalOutputTagProperty.class).get(), (OperatorVertex) edge.getDst()))
      .forEach(pair -> {
        map.putIfAbsent(pair.left(), new ArrayList<>());
        map.get(pair.left()).add(pair.right());
      });

    return map;
  }

  private List<OperatorVertex> getInternalMainOutputs(final IRVertex irVertex,
                                                      final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag) {
    return irVertexDag.getOutgoingEdgesOf(irVertex.getId())
      .stream()
      .filter(edge -> !edge.getPropertyValue(AdditionalOutputTagProperty.class).isPresent())
      .map(edge -> (OperatorVertex) edge.getDst())
      .collect(Collectors.toList());
  }

  /**
   * Return inter-task OutputWriters, for single output or output associated with main tag.
   *
   * @param irVertex                source irVertex
   * @param outEdgesToChildrenTasks outgoing edges to child tasks
   * @param intermediateDataIOFactory     intermediateDataIOFactory
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
