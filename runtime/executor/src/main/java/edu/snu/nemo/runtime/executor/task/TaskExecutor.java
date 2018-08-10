/*
 * Copyright (C) 2018 Seoul National University
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
import edu.snu.nemo.common.ir.Readable;
import edu.snu.nemo.common.ir.edge.executionproperty.AdditionalOutputTagProperty;
import edu.snu.nemo.common.ir.vertex.*;
import edu.snu.nemo.common.ir.vertex.transform.Transform;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.comm.ControlMessage;
import edu.snu.nemo.runtime.common.message.MessageEnvironment;
import edu.snu.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import edu.snu.nemo.runtime.common.plan.Task;
import edu.snu.nemo.runtime.common.plan.StageEdge;
import edu.snu.nemo.runtime.common.plan.RuntimeEdge;
import edu.snu.nemo.runtime.common.state.TaskState;
import edu.snu.nemo.runtime.executor.MetricMessageSender;
import edu.snu.nemo.runtime.executor.TaskStateManager;
import edu.snu.nemo.runtime.executor.datatransfer.*;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
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
  private final Map sideInputMap;

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
                      final MetricMessageSender metricMessageSender,
                      final PersistentConnectionToMasterMap persistentConnectionToMasterMap) {
    // Essential information
    this.isExecuted = false;
    this.taskId = task.getTaskId();
    this.taskStateManager = taskStateManager;

    // Metric sender
    this.metricMessageSender = metricMessageSender;

    // Dynamic optimization
    // Assigning null is very bad, but we are keeping this for now
    this.idOfVertexPutOnHold = null;

    this.persistentConnectionToMasterMap = persistentConnectionToMasterMap;

    // Prepare data structures
    this.sideInputMap = new HashMap();
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

   * For element-wise data processing, we traverse vertex harnesses from the roots to the leaves for each element.
   * This means that overheads associated with jumping from one harness to the other should be minimal.
   * For example, we should never perform an expensive hash operation to traverse the harnesses.
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

      final List<Boolean> isToSideInputs = children.stream()
          .map(VertexHarness::getIRVertex)
          .map(childVertex -> irVertexDag.getEdgeBetween(irVertex.getId(), childVertex.getId()))
          .map(RuntimeEdge::isSideInput)
          .collect(Collectors.toList());

      final Map<String, String> additionalOutputMap =
          getAdditionalOutputMap(irVertex, task.getTaskOutgoingEdges(), irVertexDag);
      final List<Boolean> isToAdditionalTagOutputs = children.stream()
          .map(harness -> harness.getIRVertex().getId())
          .map(additionalOutputMap::containsValue)
          .collect(Collectors.toList());

      // Handle writes
      // Main output children task writes
      final List<OutputWriter> mainChildrenTaskWriters = getMainChildrenTaskWriters(
          taskIndex, irVertex, task.getTaskOutgoingEdges(), dataTransferFactory, additionalOutputMap);
      // Additional output children task writes
      final Map<String, OutputWriter> additionalChildrenTaskWriters = getAdditionalChildrenTaskWriters(
          taskIndex, irVertex, task.getTaskOutgoingEdges(), dataTransferFactory, additionalOutputMap);
      // Find all main vertices and additional vertices
      final List<String> additionalOutputVertices = new ArrayList<>(additionalOutputMap.values());
      final Set<String> mainChildren =
          getMainOutputVertices(irVertex, irVertexDag, task.getTaskOutgoingEdges(), additionalOutputVertices);
      final OutputCollectorImpl oci = new OutputCollectorImpl(mainChildren, additionalOutputVertices);

      // intra-vertex writes
      final VertexHarness vertexHarness = new VertexHarness(irVertex, oci, children,
          isToSideInputs, isToAdditionalTagOutputs, mainChildrenTaskWriters, additionalChildrenTaskWriters,
          new ContextImpl(sideInputMap, additionalOutputMap));
      prepareTransform(vertexHarness);
      vertexIdToHarness.put(irVertex.getId(), vertexHarness);

      // Handle reads
      final boolean isToSideInput = isToSideInputs.stream().anyMatch(bool -> bool);
      if (irVertex instanceof SourceVertex) {
        dataFetcherList.add(new SourceVertexDataFetcher(
            irVertex, sourceReader.get(), vertexHarness, isToSideInput)); // Source vertex read
      }
      final List<InputReader> parentTaskReaders =
          getParentTaskReaders(taskIndex, irVertex, task.getTaskIncomingEdges(), dataTransferFactory);
      parentTaskReaders.forEach(parentTaskReader -> {
        dataFetcherList.add(new ParentTaskDataFetcher(parentTaskReader.getSrcIrVertex(), parentTaskReader,
            vertexHarness, isToSideInput)); // Parent-task read
      });
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
      outputCollector.emit(dataElement);
    } else if (irVertex instanceof OperatorVertex) {
      final Transform transform = ((OperatorVertex) irVertex).getTransform();
      transform.onData(dataElement);
    } else if (irVertex instanceof MetricCollectionBarrierVertex) {
      outputCollector.emit(dataElement);
      setIRVertexPutOnHold((MetricCollectionBarrierVertex) irVertex);
    } else {
      throw new UnsupportedOperationException("This type of IRVertex is not supported");
    }

    // Given a single input element, a vertex can produce many output elements.
    // Here, we recursively process all of the main oltput elements.
    outputCollector.iterateMain().forEach(element -> handleMainOutputElement(vertexHarness, element)); // Recursion
    outputCollector.clearMain();

    // Recursively process all of the additional output elements.
    vertexHarness.getContext().getTagToAdditionalChildren().values().forEach(tag -> {
      outputCollector.iterateTag(tag).forEach(
          element -> handleAdditionalOutputElement(vertexHarness, element, tag)); // Recursion
      outputCollector.clearTag(tag);
    });
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
   * - Phase 1: Consume task-external side-input data
   * - Phase 2: Consume task-external input data
   * - Phase 3: Finalize task-internal states and data elements
   */
  private void doExecute() {
    // Housekeeping stuff
    if (isExecuted) {
      throw new RuntimeException("Task {" + taskId + "} execution called again");
    }
    LOG.info("{} started", taskId);
    taskStateManager.onTaskStateChanged(TaskState.State.EXECUTING, Optional.empty(), Optional.empty());

    // Phase 1: Consume task-external side-input related data.
    final Map<Boolean, List<DataFetcher>> sideInputRelated = dataFetchers.stream()
        .collect(Collectors.partitioningBy(fetcher -> fetcher.isFromSideInput() || fetcher.isToSideInput()));
    if (!handleDataFetchers(sideInputRelated.get(true))) {
      return;
    }
    final Set<VertexHarness> finalizeLater = sideInputRelated.get(false).stream()
        .map(DataFetcher::getChild)
        .flatMap(vertex -> getAllReachables(vertex).stream())
        .collect(Collectors.toSet());
    for (final VertexHarness vertexHarness : sortedHarnesses) {
      if (!finalizeLater.contains(vertexHarness)) {
        finalizeVertex(vertexHarness); // finalize early to materialize intra-task side inputs.
      }
    }

    // Phase 2: Consume task-external input data.
    if (!handleDataFetchers(sideInputRelated.get(false))) {
      return;
    }

    metricMessageSender.send("TaskMetric", taskId,
        "boundedSourceReadTime", SerializationUtils.serialize(boundedSourceReadTime));
    metricMessageSender.send("TaskMetric", taskId,
        "serializedReadBytes", SerializationUtils.serialize(serializedReadBytes));
    metricMessageSender.send("TaskMetric", taskId,
        "encodedReadBytes", SerializationUtils.serialize(encodedReadBytes));

    // Phase 3: Finalize task-internal states and elements
    for (final VertexHarness vertexHarness : sortedHarnesses) {
      if (finalizeLater.contains(vertexHarness)) {
        finalizeVertex(vertexHarness);
      }
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

  private List<VertexHarness> getAllReachables(final VertexHarness src) {
    final List<VertexHarness> result = new ArrayList<>();
    result.add(src);
    result.addAll(src.getNonSideInputChildren().stream()
        .flatMap(child -> getAllReachables(child).stream()).collect(Collectors.toList()));
    result.addAll(src.getSideInputChildren().stream()
        .flatMap(child -> getAllReachables(child).stream()).collect(Collectors.toList()));
    result.addAll(src.getAdditionalTagOutputChildren().values().stream()
        .flatMap(child -> getAllReachables(child).stream()).collect(Collectors.toList()));
    return result;
  }

  private void finalizeVertex(final VertexHarness vertexHarness) {
    closeTransform(vertexHarness);
    final OutputCollectorImpl outputCollector = vertexHarness.getOutputCollector();

    // handle main outputs
    outputCollector.iterateMain().forEach(element -> handleMainOutputElement(vertexHarness, element)); // Recursion
    outputCollector.clearMain();

    // handle additional tagged outputs
    vertexHarness.getAdditionalTagOutputChildren().keySet().forEach(tag -> {
      outputCollector.iterateTag(tag).forEach(
          element -> handleAdditionalOutputElement(vertexHarness, element, tag)); // Recursion
      outputCollector.clearTag(tag);
    });
    finalizeOutputWriters(vertexHarness);
  }

  private void handleMainOutputElement(final VertexHarness harness, final Object element) {
    // writes to children tasks
    harness.getWritersToMainChildrenTasks().forEach(outputWriter -> {
      outputWriter.write(element);
    });
    // writes to side input children tasks
    if (harness.getSideInputChildren().size() > 0) {
      sideInputMap.put(((OperatorVertex) harness.getIRVertex()).getTransform().getTag(), element);
    }
    // process elements in the next vertices within a task
    harness.getNonSideInputChildren().forEach(child -> processElementRecursively(child, element));
  }

  private void handleAdditionalOutputElement(final VertexHarness harness, final Object element, final String tag) {
    // writes to additional children tasks
    harness.getWritersToAdditionalChildrenTasks().entrySet().stream()
        .filter(kv -> kv.getKey().equals(tag))
        .forEach(kv -> {
          kv.getValue().write(element);
        });
    // writes to side input children tasks
    if (harness.getSideInputChildren().size() > 0) {
      sideInputMap.put(((OperatorVertex) harness.getIRVertex()).getTransform().getTag(), element);
    }
    // process elements in the next vertices within a task
    harness.getAdditionalTagOutputChildren().entrySet().stream()
        .filter(kv -> kv.getKey().equals(tag))
        .forEach(kv -> processElementRecursively(kv.getValue(), element));
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
          LOG.error("{} Execution Failed (Recoverable: input read failure)! Exception: {}", taskId, e.toString());
          return false;
        }

        // Successfully fetched an element
        if (dataFetcher.isFromSideInput()) {
          sideInputMap.put(((OperatorVertex) dataFetcher.getDataSource()).getTransform().getTag(), element);
        } else {
          processElementRecursively(dataFetcher.getChild(), element);
        }
      }

      // Remove the finished fetcher from the list
      if (finishedFetcherIndex != NONE_FINISHED) {
        availableFetchers.remove(finishedFetcherIndex);
      }
    }
    return true;
  }

  ////////////////////////////////////////////// Helper methods for setting up initial data structures

  private Map<String, String> getAdditionalOutputMap(final IRVertex irVertex,
                                                     final List<StageEdge> outEdgesToChildrenTasks,
                                                     final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag) {
    final Map<String, String> additionalOutputMap = new HashMap<>();

    // Add all intra-task additional tags to additional output map.
    irVertexDag.getOutgoingEdgesOf(irVertex.getId())
        .stream()
        .filter(edge -> edge.getPropertyValue(AdditionalOutputTagProperty.class).isPresent())
        .map(edge ->
            Pair.of(edge.getPropertyValue(AdditionalOutputTagProperty.class).get(), edge.getDst().getId()))
        .forEach(pair -> additionalOutputMap.put(pair.left(), pair.right()));

    // Add all inter-task additional tags to additional output map.
    outEdgesToChildrenTasks
        .stream()
        .filter(edge -> edge.getSrcIRVertex().getId().equals(irVertex.getId()))
        .filter(edge -> edge.getPropertyValue(AdditionalOutputTagProperty.class).isPresent())
        .map(edge ->
            Pair.of(edge.getPropertyValue(AdditionalOutputTagProperty.class).get(), edge.getDstIRVertex().getId()))
        .forEach(pair -> additionalOutputMap.put(pair.left(), pair.right()));

    return additionalOutputMap;
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
                                                 final IRVertex irVertex,
                                                 final List<StageEdge> inEdgesFromParentTasks,
                                                 final DataTransferFactory dataTransferFactory) {
    return inEdgesFromParentTasks
        .stream()
        .filter(inEdge -> inEdge.getDstIRVertex().getId().equals(irVertex.getId()))
        .map(inEdgeForThisVertex -> dataTransferFactory
            .createReader(taskIndex, inEdgeForThisVertex.getSrcIRVertex(), inEdgeForThisVertex))
        .collect(Collectors.toList());
  }

  private Set<String> getMainOutputVertices(final IRVertex irVertex,
                                             final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
                                             final List<StageEdge> outEdgesToChildrenTasks,
                                             final List<String> additionalOutputVertices) {
    // all intra-task children vertices id
    final List<String> outputVertices = irVertexDag.getOutgoingEdgesOf(irVertex).stream()
        .filter(edge -> edge.getSrc().getId().equals(irVertex.getId()))
        .map(edge -> edge.getDst().getId())
        .collect(Collectors.toList());

    // all inter-task children vertices id
    outputVertices
        .addAll(outEdgesToChildrenTasks.stream()
            .filter(edge -> edge.getSrcIRVertex().getId().equals(irVertex.getId()))
            .map(edge -> edge.getDstIRVertex().getId())
            .collect(Collectors.toList()));

    // return vertices that are not marked as additional tagged outputs
    return new HashSet<>(outputVertices.stream()
        .filter(vertexId -> !additionalOutputVertices.contains(vertexId))
        .collect(Collectors.toList()));
  }

  /**
   * Return inter-task OutputWriters, for single output or output associated with main tag.
   * @param taskIndex               current task index
   * @param irVertex                source irVertex
   * @param outEdgesToChildrenTasks outgoing edges to child tasks
   * @param dataTransferFactory     dataTransferFactory
   * @param taggedOutputs           tag to vertex id map
   * @return OutputWriters for main children tasks
   */
  private List<OutputWriter> getMainChildrenTaskWriters(final int taskIndex,
                                                        final IRVertex irVertex,
                                                        final List<StageEdge> outEdgesToChildrenTasks,
                                                        final DataTransferFactory dataTransferFactory,
                                                        final Map<String, String> taggedOutputs) {
    return outEdgesToChildrenTasks
        .stream()
        .filter(outEdge -> outEdge.getSrcIRVertex().getId().equals(irVertex.getId()))
        .filter(outEdge -> !taggedOutputs.containsValue(outEdge.getDstIRVertex().getId()))
        .map(outEdgeForThisVertex -> dataTransferFactory
            .createWriter(irVertex, taskIndex, outEdgeForThisVertex.getDstIRVertex(), outEdgeForThisVertex))
        .collect(Collectors.toList());
  }

  /**
   * Return inter-task OutputWriters associated with additional output tags.
   * @param taskIndex               current task index
   * @param irVertex                source irVertex
   * @param outEdgesToChildrenTasks outgoing edges to child tasks
   * @param dataTransferFactory     dataTransferFactory
   * @param taggedOutputs           tag to vertex id map
   * @return additional children vertex id to OutputWriters map.
   */
  private Map<String, OutputWriter> getAdditionalChildrenTaskWriters(final int taskIndex,
                                                                     final IRVertex irVertex,
                                                                     final List<StageEdge> outEdgesToChildrenTasks,
                                                                     final DataTransferFactory dataTransferFactory,
                                                                     final Map<String, String> taggedOutputs) {
    final Map<String, OutputWriter> additionalChildrenTaskWriters = new HashMap<>();

    outEdgesToChildrenTasks
        .stream()
        .filter(outEdge -> outEdge.getSrcIRVertex().getId().equals(irVertex.getId()))
        .filter(outEdge -> taggedOutputs.containsValue(outEdge.getDstIRVertex().getId()))
        .forEach(outEdgeForThisVertex -> {
          additionalChildrenTaskWriters.put(outEdgeForThisVertex.getDstIRVertex().getId(),
              dataTransferFactory.createWriter(irVertex, taskIndex, outEdgeForThisVertex.getDstIRVertex(),
                  outEdgeForThisVertex));
        });

    return additionalChildrenTaskWriters;
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
    vertexHarness.getContext().getSerializedData().ifPresent(data ->
        persistentConnectionToMasterMap.getMessageSender(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID).send(
            ControlMessage.Message.newBuilder()
                .setId(RuntimeIdGenerator.generateMessageId())
                .setListenerId(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID)
                .setType(ControlMessage.MessageType.ExecutorDataCollected)
                .setDataCollected(ControlMessage.DataCollectMessage.newBuilder().setData(data).build())
                .build()));
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

    // finalize OutputWriters for main children
    vertexHarness.getWritersToMainChildrenTasks().forEach(outputWriter -> {
      outputWriter.close();
      final Optional<Long> writtenBytes = outputWriter.getWrittenBytes();
      writtenBytes.ifPresent(writtenBytesList::add);
    });

    // finalize OutputWriters for additional tagged children
    vertexHarness.getWritersToAdditionalChildrenTasks().values().forEach(outputWriter -> {
      outputWriter.close();

      final Optional<Long> writtenBytes = outputWriter.getWrittenBytes();
      writtenBytes.ifPresent(writtenBytesList::add);
    });

    long totalWrittenBytes = 0;
    for (final Long writtenBytes : writtenBytesList) {
      totalWrittenBytes += writtenBytes;
    }
    metricMessageSender.send("TaskMetric", taskId,
        "writtenBytes", SerializationUtils.serialize(totalWrittenBytes));
  }
}
