/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.runtime.executor.task;

import com.google.common.collect.Lists;
import edu.snu.nemo.common.ContextImpl;
import edu.snu.nemo.common.Pair;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.OutputCollector;
import edu.snu.nemo.common.ir.Readable;
import edu.snu.nemo.common.ir.vertex.*;
import edu.snu.nemo.common.ir.vertex.transform.Transform;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.plan.Task;
import edu.snu.nemo.runtime.common.plan.StageEdge;
import edu.snu.nemo.runtime.common.plan.RuntimeEdge;
import edu.snu.nemo.runtime.common.state.TaskState;
import edu.snu.nemo.runtime.executor.MetricCollector;
import edu.snu.nemo.runtime.executor.MetricMessageSender;
import edu.snu.nemo.runtime.executor.TaskStateManager;
import edu.snu.nemo.runtime.executor.datatransfer.*;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
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
  private final List<DataFetcher> dataFetchers;
  private final List<VertexHarness> sortedHarnesses;

  // Metrics information
  private final Map<String, Object> metricMap;
  private final MetricCollector metricCollector;

  // Dynamic optimization
  private String idOfVertexPutOnHold;

  /**
   * Constructor.
   * @param task Task with information needed during execution.
   * @param irVertexDag A DAG of vertices.
   * @param taskStateManager State manager for this Task.
   * @param dataTransferFactory For reading from/writing to data to other tasks.
   * @param metricMessageSender For sending metric with execution stats to Master.
   */
  public TaskExecutor(final Task task,
                      final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
                      final TaskStateManager taskStateManager,
                      final DataTransferFactory dataTransferFactory,
                      final MetricMessageSender metricMessageSender) {
    // Essential information
    this.isExecuted = false;
    this.taskId = task.getTaskId();
    this.taskStateManager = taskStateManager;

    // Metrics information
    this.metricMap = new HashMap<>();
    this.metricCollector = new MetricCollector(metricMessageSender);

    // Dynamic optimization
    // Assigning null is very bad, but we are keeping this for now
    this.idOfVertexPutOnHold = null;

    // Prepare data structures
    final Pair<List<DataFetcher>, List<VertexHarness>> pair = prepare(task, irVertexDag, dataTransferFactory);
    this.dataFetchers = pair.left();
    this.sortedHarnesses = pair.right();
  }

  /**
   * Converts the DAG of vertices into pointer-based DAG of vertex harnesses.
   * This conversion is necessary for constructing concrete data channels for each vertex's inputs and outputs.
   *
   * - Source vertex read: Explicitly handled (SourceVertexDataFetcher)
   * - Sink vertex write: Implicitly handled within the vertex
   *
   * - Parent-task read: Explicitly handled (ParentTaskDataFetcher)
   * - Children-task write: Explicitly handled (VertexHarness)
   *
   * - Intra-task read: Implicitly handled when performing Intra-task writes
   * - Intra-task write: Explicitly handled (VertexHarness)
   *
   * For element-wise data processing, we traverse vertex harnesses from the roots to the leaves for each element.
   * This means that overheads associated with jumping from one harness to the other should be minimal.
   * For example, we should never perform an expensive hash operation while traversing the harnesses.
   *
   * @param task task.
   * @param irVertexDag dag.
   * @return fetchers and harnesses.
   */
  private Pair<List<DataFetcher>, List<VertexHarness>> prepare(final Task task,
                                                               final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
                                                               final DataTransferFactory dataTransferFactory) {
    final int taskIndex = RuntimeIdGenerator.getIndexFromTaskId(task.getTaskId());

    // Traverse in a reverse-topological order to ensure that each visited vertex's children vertices exist.
    final List<IRVertex> reverseTopologicallySorted = Lists.reverse(irVertexDag.getTopologicalSort());

    // Create a harness for each vertex
    final List<DataFetcher> dataFetcherList = new ArrayList<>();
    final Map<String, VertexHarness> vertexIdToHarness = new HashMap<>();
    reverseTopologicallySorted.forEach(irVertex -> {
      final List<VertexHarness> children = getChildrenHarnesses(irVertex, irVertexDag, vertexIdToHarness);
      final Optional<Readable> sourceReader = getSourceVertexReader(irVertex, task.getIrVertexIdToReadable());
      if (sourceReader.isPresent() != irVertex instanceof SourceVertex) {
        throw new IllegalStateException(irVertex.toString());
      }

      final List<Boolean> isSideInputs = children.stream()
          .map(VertexHarness::getIRVertex)
          .map(childVertex -> irVertexDag.getEdgeBetween(irVertex.getId(), childVertex.getId()))
          .map(RuntimeEdge::isSideInput)
          .collect(Collectors.toList());

      // Handle writes
      final List<OutputWriter> childrenTaskWriters = getChildrenTaskWriters(
          taskIndex, irVertex, task.getTaskOutgoingEdges(), dataTransferFactory); // Children-task write
      final VertexHarness vertexHarness = new VertexHarness(irVertex, new OutputCollectorImpl(), children,
          isSideInputs, childrenTaskWriters, new ContextImpl(new HashMap())); // Intra-vertex write
      prepareTransform(vertexHarness);
      vertexIdToHarness.put(irVertex.getId(), vertexHarness);

      // Handle reads
      if (irVertex instanceof SourceVertex) {
        dataFetcherList.add(new SourceVertexDataFetcher(
            sourceReader.get(), Arrays.asList(vertexHarness), metricMap)); // Source vertex read
      }
      final List<InputReader> parentTaskReaders =
          getParentTaskReaders(taskIndex, irVertex, task.getTaskIncomingEdges(), dataTransferFactory);
      if (parentTaskReaders.size() > 0) {
        dataFetcherList.add(new ParentTaskDataFetcher(
            parentTaskReaders, Arrays.asList(vertexHarness), metricMap)); // Parent-task read
      }
    });

    final List<VertexHarness> sortedHarnessList = irVertexDag.getTopologicalSort()
        .stream()
        .map(vertex -> vertexIdToHarness.get(vertex.getId()))
        .collect(Collectors.toList());

    return Pair.of(dataFetcherList, sortedHarnessList);
  }

  /**
   * Recursively process a data element down the DAG dependency.
   * @param vertexHarness VertexHarness of a vertex to execute.
   * @param dataElement input data element to process.
   */
  private void processElementRecursively(final VertexHarness vertexHarness, final Object dataElement) {
    final IRVertex irVertex = vertexHarness.getIRVertex();
    final OutputCollectorImpl outputCollector = vertexHarness.getOutputCollector();
    if (irVertex instanceof SourceVertex) {
      relayElement(outputCollector, dataElement);
    } else if (irVertex instanceof OperatorVertex) {
      final Transform transform = ((OperatorVertex) irVertex).getTransform();
      transform.onData(dataElement);
    } else if (irVertex instanceof MetricCollectionBarrierVertex) {
      relayElement(outputCollector, dataElement);
      setIRVertexPutOnHold((MetricCollectionBarrierVertex) irVertex);
    } else {
      throw new UnsupportedOperationException("This type of IRVertex is not supported");
    }

    // Given a single input element, a vertex can produce many output elements.
    // Here, we recursively process all of the output elements.
    while (!outputCollector.isEmpty()) {
      final Object element = outputCollector.remove();
      vertexHarness.getWritersToChildrenTasks().forEach(outputWriter -> outputWriter.write(element));
      vertexHarness.getSideInputChildren().forEach(child -> child.getContext().getSideInputs().put(irVertex, element));
      vertexHarness.getNonSideInputChildren().forEach(child -> processElementRecursively(child, element)); // Recursion
    }
  }

  private void relayElement(final OutputCollector outputCollector, final Object dataElement) {
    if (dataElement == null) { // null used for Beam VoidCoders
      final List<Object> nullForVoidCoder = Collections.singletonList(dataElement);
      outputCollector.emit(nullForVoidCoder);
    } else {
      outputCollector.emit(dataElement);
    }
  }

  /**
   * Execute a task, while handling unrecoverable errors and exceptions.
   */
  public void execute() {
    try {
      doExecute();
    } catch (Throwable throwable) {
      // ANY uncaught throwable is reported to the master
      taskStateManager.onTaskStateChanged(TaskState.State.FAILED_UNRECOVERABLE, Optional.empty(), Optional.empty());
      LOG.error("{} Execution Failed! Exception: {}", taskId, throwable.toString());
    }
  }

  /**
   * The task is executed in the following two phases.
   * - Phase 1: Consume task-external input data
   * - Phase 2: Finalize task-internal states and data elements
   */
  private void doExecute() {
    if (isExecuted) {
      throw new RuntimeException("Task {" + taskId + "} execution called again");
    }
    LOG.info("{} started", taskId);
    taskStateManager.onTaskStateChanged(TaskState.State.EXECUTING, Optional.empty(), Optional.empty());

    // Metric start
    metricCollector.beginMeasurement(taskId, metricMap);

    // Phase 1: Consume task-external input data.
    // Recursively process each data element produced by a data fetcher.
    final List<DataFetcher> availableFetchers = new ArrayList<>(dataFetchers);
    int finishedFetcherIndex = NONE_FINISHED;
    while (!availableFetchers.isEmpty()) { // empty means we've consumed all task-external input data
      for (int i = 0; i < availableFetchers.size(); i++) {
        final DataFetcher dataFetcher = dataFetchers.get(i);
        final Object element;
        try {
          element = dataFetcher.fetchDataElement();
        } catch (IOException e) {
          taskStateManager.onTaskStateChanged(TaskState.State.FAILED_RECOVERABLE,
              Optional.empty(), Optional.of(TaskState.RecoverableFailureCause.INPUT_READ_FAILURE));
          LOG.error("{} Execution Failed (Recoverable: input read failure)! Exception: {}", taskId, e.toString());
          return;
        }

        if (element == null) {
          finishedFetcherIndex = i;
          break;
        } else {
          for (final VertexHarness harness : dataFetcher.getConsumers()) {
            processElementRecursively(harness, element);
          }
        }
      }

      // Remove the finished fetcher from the list
      if (finishedFetcherIndex != NONE_FINISHED) {
        availableFetchers.remove(finishedFetcherIndex);
      }
    }

    // Phase 2: Finalize task-internal states and elements
    // Topologically close each harness's transform, and recursively process any resulting data.
    for (final VertexHarness vertexHarness : sortedHarnesses) {
      closeTransform(vertexHarness);
      while (!vertexHarness.getOutputCollector().isEmpty()) {
        final Object element = vertexHarness.getOutputCollector().remove();
        for (final VertexHarness harness : vertexHarness.getNonSideInputChildren()) {
          processElementRecursively(harness, element);
        }
        finalizeOutputWriters(vertexHarness);
      }
    }

    // Metric done
    metricCollector.endMeasurement(taskId, metricMap);

    // DynOpt
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

  ////////////////////////////////////////////// Helper methods for setting up initial data structures

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
                                                 final IRVertex irVertex,
                                                 final List<StageEdge> inEdgesFromParentTasks,
                                                 final DataTransferFactory dataTransferFactory) {
    return inEdgesFromParentTasks
        .stream()
        .filter(inEdge -> inEdge.getDstVertex().getId().equals(irVertex.getId()))
        .map(inEdgeForThisVertex -> dataTransferFactory
            .createReader(taskIndex, inEdgeForThisVertex.getSrcVertex(), inEdgeForThisVertex))
        .collect(Collectors.toList());
  }

  private List<OutputWriter> getChildrenTaskWriters(final int taskIndex,
                                                    final IRVertex irVertex,
                                                    final List<StageEdge> outEdgesToChildrenTasks,
                                                    final DataTransferFactory dataTransferFactory) {
    return outEdgesToChildrenTasks
        .stream()
        .filter(outEdge -> outEdge.getSrcVertex().getId().equals(irVertex.getId()))
        .map(outEdgeForThisVertex -> dataTransferFactory
            .createWriter(irVertex, taskIndex, outEdgeForThisVertex.getDstVertex(), outEdgeForThisVertex))
        .collect(Collectors.toList());
  }

  private List<VertexHarness> getChildrenHarnesses(final IRVertex irVertex,
                                                   final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
                                                   final Map<String, VertexHarness> vertexIdToHarness) {
    final List<VertexHarness> childrenHandlers = irVertexDag.getChildren(irVertex.getId())
        .stream()
        .map(IRVertex::getId)
        .map(vertexIdToHarness::get)
        .collect(Collectors.toList());
    if (childrenHandlers.stream().anyMatch(harness -> harness == null)) {
      // Sanity check: there shouldn't be a null harness.
      throw new IllegalStateException(childrenHandlers.toString());
    }
    return childrenHandlers;
  }

  ////////////////////////////////////////////// Transform-specific helper methods

  private void prepareTransform(final VertexHarness vertexHarness) {
    final IRVertex irVertex = vertexHarness.getIRVertex();
    if (irVertex instanceof OperatorVertex) {
      final Transform transform = ((OperatorVertex) irVertex).getTransform();
      transform.prepare(vertexHarness.getContext(), vertexHarness.getOutputCollector());
    }
  }

  private void closeTransform(final VertexHarness vertexHarness) {
    final IRVertex irVertex = vertexHarness.getIRVertex();
    if (irVertex instanceof OperatorVertex) {
      Transform transform = ((OperatorVertex) irVertex).getTransform();
      transform.close();
    }
  }

  ////////////////////////////////////////////// Misc

  private void setIRVertexPutOnHold(final MetricCollectionBarrierVertex irVertex) {
    idOfVertexPutOnHold = irVertex.getId();
  }

  /**
   * Finalize the output write of this vertex.
   * As element-wise output write is done and the block is in memory,
   * flush the block into the designated data store and commit it.
   * @param vertexHarness harness.
   */
  private void finalizeOutputWriters(final VertexHarness vertexHarness) {
    final List<Long> writtenBytesList = new ArrayList<>();
    final Map<String, Object> metric = new HashMap<>();
    final IRVertex irVertex = vertexHarness.getIRVertex();

    metricCollector.beginMeasurement(irVertex.getId(), metric);
    final long writeStartTime = System.currentTimeMillis();

    vertexHarness.getWritersToChildrenTasks().forEach(outputWriter -> {
      outputWriter.close();
      final Optional<Long> writtenBytes = outputWriter.getWrittenBytes();
      writtenBytes.ifPresent(writtenBytesList::add);
    });

    final long writeEndTime = System.currentTimeMillis();
    metric.put("OutputWriteTime(ms)", writeEndTime - writeStartTime);
    if (!writtenBytesList.isEmpty()) {
      long totalWrittenBytes = 0;
      for (final Long writtenBytes : writtenBytesList) {
        totalWrittenBytes += writtenBytes;
      }
      metricMap.put("WrittenBytes", totalWrittenBytes);
    }
    metricCollector.endMeasurement(irVertex.getId(), metric);
  }

}
