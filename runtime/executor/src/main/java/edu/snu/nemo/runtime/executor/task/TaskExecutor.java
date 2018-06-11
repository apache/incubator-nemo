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
import edu.snu.nemo.common.exception.BlockFetchException;
import edu.snu.nemo.common.exception.BlockWriteException;
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
import edu.snu.nemo.runtime.executor.data.DataUtil;
import edu.snu.nemo.runtime.executor.datatransfer.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Executes a task.
 * Should be accessed by a single thread.
 */
@NotThreadSafe
final class TaskExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(TaskExecutor.class.getName());

  // Essential information
  private final Task task;
  private final String taskId;
  private final TaskStateManager taskStateManager;
  private final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag;

  // Data handlers for the root vertices of the task.
  private final List<RootDataHandler> rootDataHandlers;

  // Data handling information
  private final Map<String, Iterator> iteratorIdToSrcIterator;
  private final Map<String, List<DataHandler>> srcIteratorIdToDataHandlersMap;
  private final Map<String, List<DataHandler>> iteratorIdToDataHandlersMap;

  // Metrics information
  private final MetricCollector metricCollector;
  private long serBlockSize;
  private long encodedBlockSize;

  // Actual data
  private final LinkedBlockingQueue<Pair<String, DataUtil.IteratorWithNumBytes>> vertexIdAndDataPairQueue;
  private int numPartitionsFromOtherStages;

  // Misc
  private final DataTransferFactory channelFactory;
  private boolean isExecuted;

  // Dynamic optimization
  private String idOfVertexPutOnHold;

  /**
   * Constructor.
   * @param task Task with information needed during execution.
   * @param irVertexDag A DAG of vertices.
   * @param taskStateManager State manager for this Task.
   * @param channelFactory For reading from/writing to data to other Stages.
   * @param metricMessageSender For sending metric with execution stats to Master.
   */
  public TaskExecutor(final Task task,
                      final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
                      final TaskStateManager taskStateManager,
                      final DataTransferFactory channelFactory,
                      final MetricMessageSender metricMessageSender) {
    // Information from the Task.
    this.taskId = task.getTaskId();
    this.taskStateManager = taskStateManager;
    this.irVertexDag = irVertexDag;

    // Data handling information
    this.iteratorIdToSrcIterator = new HashMap<>();
    this.srcIteratorIdToDataHandlersMap = new HashMap<>();
    this.iteratorIdToDataHandlersMap = new HashMap<>();
    this.vertexIdToDataHandler = new HashMap<>();

    // Metrics information
    this.metricCollector = new MetricCollector(metricMessageSender);
    this.serBlockSize = 0;
    this.encodedBlockSize = 0;

    // Actual data
    this.vertexIdAndDataPairQueue = new LinkedBlockingQueue<>();
    this.numPartitionsFromOtherStages = 0;

    // Misc
    this.channelFactory = channelFactory;
    this.finishedVertexIds = new HashSet<>();
    this.isExecuted = false;

    // Dynamic optimization
    // Assigning null is very bad, but we are keeping this for now
    this.idOfVertexPutOnHold = null;
  }

  private boolean prepareTransform(final IRVertex irVertex) {
    if (irVertex instanceof OperatorVertex) {
      final Transform transform = ((OperatorVertex) irVertex).getTransform();
      final Map<Transform, Object> sideInputMap = new HashMap<>();
      final DataHandler dataHandler = vertexIdToDataHandler.get(irVertex.getId());
      // Check and collect side inputs.
      if (!dataHandler.getSideInputFromOtherStages().isEmpty()) {
        sideInputFromOtherStages(irVertex, sideInputMap);
      }
      if (!dataHandler.getSideInputFromThisStage().isEmpty()) {
        sideInputFromThisStage(irVertex, sideInputMap);
      }

      final Transform.Context transformContext = new ContextImpl(sideInputMap);
      final OutputCollectorImpl outputCollector = dataHandler.getOutputCollector();
      transform.prepare(transformContext, outputCollector);
    }
  }

  /**
   * Initializes this Task before execution.
   * 1) Create and connect reader/writers for both inter-Task data and intra-Task data.
   * 2) Prepares Transforms if needed.
   */
  public void prepare() {
    // Initialize data handlers for each IRVertex.
    final Map<String, DataHandler> vertexIdToDataHandler = new HashMap<>();
    final HashSet<IRVertex> rootVertices = new HashSet<>(irVertexDag.getRootVertices());

    // Traverse in a reverse-topological order to ensure that each visited vertex's children vertices exist.
    final List<IRVertex> reverseTopologicallySorted = Lists.reverse(irVertexDag.getTopologicalSort());
    reverseTopologicallySorted.forEach(irVertex -> {
      // Get children handlers.
      final List<DataHandler> childrenHandlers = irVertexDag.getChildren(irVertex.getId())
          .stream()
          .map(IRVertex::getId)
          .map(vertexIdToDataHandler::get)
          .collect(Collectors.toList());
      if (childrenHandlers.stream().anyMatch(handler -> handler == null)) {
        // Sanity check: there shouldn't be a null handler.
        throw new IllegalStateException(childrenHandlers.toString());
      }

      // Create a data handler.
      final DataHandler dataHandler;
      if (rootVertices.contains(irVertex)) {
        // This is a root vertex.
        dataHandler = new RootDataHandler();
        inEdgesFromOtherStages.forEach(stageEdge -> {
          final InputReader inputReader = channelFactory.createReader(taskIndex, stageEdge.getSrcVertex(), stageEdge);

          // For InputReaders that have side input, collect them separately.
          if (inputReader.isSideInputReader()) {
            dataHandler.addSideInputFromOtherStages(inputReader);
          } else {
            inputReaderToDataHandlers.putIfAbsent(inputReader, new ArrayList<>());
            inputReaderToDataHandlers.get(inputReader).add(dataHandler);
          }
        });
      } else {
        // This is a non-root vertex.


        dataHandler = new NonRootDataHandler(irVertex, );

        outEdgesToOtherStages.forEach(stageEdge -> {
          final OutputWriter outputWriter = channelFactory.createWriter(
              irVertex, taskIndex, stageEdge.getDstVertex(), stageEdge);
          dataHandler.addOutputWriter(outputWriter);
        });
      }

      // Remember the created data handler
      vertexIdToDataHandler.put(irVertex.getId(), dataHandler);
    });



    // Initialize data transfer.
    // Construct a pointer-based DAG of vertexIdToDataHandler that are used for data transfer.
    // 'Pointer-based' means that it isn't Map/List-based in getting the data structure or parent/children
    // to avoid element-wise extra overhead of calculating hash values(HashMap) or iterating Lists.
    final Map<InputReader, List<DataHandler>> inputReaderToDataHandlers = new HashMap<>();
    final int taskIndex = RuntimeIdGenerator.getIndexFromTaskId(taskId);
    irVertexDag.topologicalDo(irVertex -> {
      final Set<StageEdge> inEdgesFromOtherStages = filterEdgesTo(irVertex);
      final Set<StageEdge> outEdgesToOtherStages = filterEdgesFrom(irVertex);
      final DataHandler dataHandler = vertexIdToDataHandler.get(irVertex.getId());

      // Set data handlers of children irVertices.
      // This forms a pointer-based DAG of vertexIdToDataHandler.
      final List<DataHandler> childrenDataHandlers = new ArrayList<>();
      irVertexDag.getChildren(irVertex.getId()).forEach(child ->
          childrenDataHandlers.add(vertexIdToDataHandler.get(child.getId())));
      dataHandler.setChildrenDataHandler(childrenDataHandlers);

      // Add InputPipes for intra-stage data transfer
      addInputFromThisStage(irVertex, dataHandler);

      // Add OutputPipe for intra-stage data transfer
      setOutputCollector(irVertex, dataHandler);
    });
  }

  /**
   * Executes the task.
   */
  public void execute() {
    if (isExecuted) {
      throw new RuntimeException("Task {" + taskId + "} execution called again!");
    }




    final Map<String, Object> metric = new HashMap<>();
    metricCollector.beginMeasurement(taskId, metric);
    long boundedSrcReadStartTime = 0;
    long boundedSrcReadEndTime = 0;
    long inputReadStartTime = 0;
    long inputReadEndTime = 0;
    isExecuted = true;
    taskStateManager.onTaskStateChanged(TaskState.State.EXECUTING, Optional.empty(), Optional.empty());
    LOG.info("{} Executing!", taskId);

    // Prepare input data from bounded source.
    boundedSrcReadStartTime = System.currentTimeMillis();
    prepareInputFromSource();
    boundedSrcReadEndTime = System.currentTimeMillis();
    metric.put("BoundedSourceReadTime(ms)", boundedSrcReadEndTime - boundedSrcReadStartTime);

    // Prepare input data from other stages.
    inputReadStartTime = System.currentTimeMillis();
    prepareInputFromOtherStages();

    // Execute the IRVertex DAG.
    try {
      srcIteratorIdToDataHandlersMap.forEach((srcIteratorId, dataHandlers) -> {
        Iterator iterator = iteratorIdToSrcIterator.get(srcIteratorId);
        iterator.forEachRemaining(element -> {
          for (final DataHandler dataHandler : dataHandlers) {
            processElementRecursively(dataHandler, element);
          }
        });
      });

      // Process data from other stages.
      for (int currPartition = 0; currPartition < numPartitionsFromOtherStages; currPartition++) {
        Pair<String, DataUtil.IteratorWithNumBytes> idToIteratorPair = vertexIdAndDataPairQueue.take();
        final String iteratorId = idToIteratorPair.left();
        final DataUtil.IteratorWithNumBytes iterator = idToIteratorPair.right();
        List<DataHandler> dataHandlers = iteratorIdToDataHandlersMap.get(iteratorId);
        iterator.forEachRemaining(element -> {
          for (final DataHandler dataHandler : dataHandlers) {
            processElementRecursively(dataHandler, element);
          }
        });

        // Collect metrics on block size if possible.
        try {
          serBlockSize += iterator.getNumSerializedBytes();
        } catch (final DataUtil.IteratorWithNumBytes.NumBytesNotSupportedException e) {
          serBlockSize = -1;
        } catch (final IllegalStateException e) {
          LOG.error("Failed to get the number of bytes of serialized data - the data is not ready yet ", e);
        }
        try {
          encodedBlockSize += iterator.getNumEncodedBytes();
        } catch (final DataUtil.IteratorWithNumBytes.NumBytesNotSupportedException e) {
          encodedBlockSize = -1;
        } catch (final IllegalStateException e) {
          LOG.error("Failed to get the number of bytes of encoded data - the data is not ready yet ", e);
        }
      }
      inputReadEndTime = System.currentTimeMillis();
      metric.put("InputReadTime(ms)", inputReadEndTime - inputReadStartTime);

      // Process intra-Task data.
      // Intra-Task data comes from outputCollectors of this Task's vertices.
      initializeOutputToChildrenDataHandlersMap();
      while (!finishedAllVertices()) {
        outputToChildrenDataHandlers.forEach((outputCollector, childrenDataHandlers) -> {
          // Get the vertex that has this outputCollector as its output outputCollector
          final IRVertex outputProducer = vertexIdToDataHandler.values().stream()
              .filter(dataHandler -> dataHandler.getOutputCollector() == outputCollector)
              .findFirst().get().getIRVertex();

          // Before consuming the output of outputProducer as input,
          // close transform if it is OperatorTransform.
          closeTransform(outputProducer);

          // Set outputProducer as finished.
          finishedVertexIds.add(outputProducer.getId());

          while (!outputCollector.isEmpty()) {
            final Object element = outputCollector.remove();

            // Pass outputProducer's output to its children tasks recursively.
            if (!childrenDataHandlers.isEmpty()) {
              for (final DataHandler childDataHandler : childrenDataHandlers) {
                processElementRecursively(childDataHandler, element);
              }
            }

            // Write element-wise to OutputWriters if any and close the OutputWriters.
            if (hasOutputWriter(outputProducer)) {
              // If outputCollector isn't empty(if closeTransform produced some output),
              // write them element-wise to OutputWriters.

              // TODO: per-element op
              List<OutputWriter> outputWritersOfTask =
                  getDataHandler(outputProducer).getOutputWriters();
              outputWritersOfTask.forEach(outputWriter -> outputWriter.write(element));
            }
          }

          if (hasOutputWriter(outputProducer)) {
            writeAndCloseOutputWriters(outputProducer);
          }
        });
        updateOutputToChildrenDataHandlersMap();
      }
    } catch (final BlockWriteException ex2) {
      taskStateManager.onTaskStateChanged(TaskState.State.FAILED_RECOVERABLE,
          Optional.empty(), Optional.of(TaskState.RecoverableFailureCause.OUTPUT_WRITE_FAILURE));
      LOG.error("{} Execution Failed (Recoverable: output write failure)! Exception: {}",
          taskId, ex2.toString());
    } catch (final Exception e) {
      taskStateManager.onTaskStateChanged(TaskState.State.FAILED_UNRECOVERABLE,
          Optional.empty(), Optional.empty());
      LOG.error("{} Execution Failed! Exception: {}",
          taskId, e.toString());
      throw new RuntimeException(e);
    }

    // Put Task-unit metrics.
    final boolean available = serBlockSize >= 0;
    putReadBytesMetric(available, serBlockSize, encodedBlockSize, metric);
    metricCollector.endMeasurement(taskId, metric);
    if (idOfVertexPutOnHold == null) {
      taskStateManager.onTaskStateChanged(TaskState.State.COMPLETE, Optional.empty(), Optional.empty());
    } else {
      taskStateManager.onTaskStateChanged(TaskState.State.ON_HOLD,
          Optional.of(idOfVertexPutOnHold),
          Optional.empty());
    }
    LOG.info("{} Complete!", taskId);
  }

  private Set<StageEdge> filterEdgesTo(final List<StageEdge> edges,
                                       final IRVertex irVertex) {
    return edges.stream().filter(
        stageInEdge -> stageInEdge.getDstVertex().getId().equals(irVertex.getId()))
        .collect(Collectors.toSet());
  }

  private Set<StageEdge> filterEdgesFrom(final IRVertex irVertex) {
    return stageOutgoingEdges.stream().filter(
        stageInEdge -> stageInEdge.getSrcVertex().getId().equals(irVertex.getId()))
        .collect(Collectors.toSet());
  }

  /**
   * Add input OutputCollectors to each {@link IRVertex}.
   * Input OutputCollector denotes all the OutputCollectors of intra-Stage dependencies.
   *
   * @param irVertex the IRVertex to add input OutputCollectors to.
   */
  private void addInputFromThisStage(final IRVertex irVertex, final DataHandler dataHandler) {
    List<IRVertex> parentVertices = irVertexDag.getParents(irVertex.getId());
    if (parentVertices != null) {
      parentVertices.forEach(parent -> {
        final OutputCollectorImpl parentOutputCollector = vertexIdToDataHandler.get(parent.getId()).getOutputCollector();
        if (parentOutputCollector.hasSideInputFor(irVertex.getId())) {
          dataHandler.addSideInputFromThisStage(parentOutputCollector);
        } else {
          dataHandler.addInputFromThisStages(parentOutputCollector);
        }
      });
    }
  }

  /**
   * Add outputCollectors to each {@link IRVertex}.
   * @param irVertex the IRVertex to add output outputCollectors to.
   */
  private void setOutputCollector(final IRVertex irVertex, final DataHandler dataHandler) {
    final OutputCollectorImpl outputCollector = new OutputCollectorImpl();
    irVertexDag.getOutgoingEdgesOf(irVertex).forEach(outEdge -> {
      if (outEdge.isSideInput()) {
        outputCollector.setSideInputRuntimeEdge(outEdge);
        outputCollector.setAsSideInputFor(irVertex.getId());
      }
    });

    dataHandler.setOutputCollector(outputCollector);
  }
  private void setIRVertexPutOnHold(final MetricCollectionBarrierVertex irVertex) {
    idOfVertexPutOnHold = irVertex.getId();
  }

  /**
   * Finalize the output write of this Task.
   * As element-wise output write is done and the block is in memory,
   * flush the block into the designated data store and commit it.
   *
   * @param irVertex the IRVertex with OutputWriter to flush and commit output block.
   */
  private void writeAndCloseOutputWriters(final IRVertex irVertex) {
    final List<Long> writtenBytesList = new ArrayList<>();
    final Map<String, Object> metric = new HashMap<>();
    metricCollector.beginMeasurement(irVertex.getId(), metric);
    final long writeStartTime = System.currentTimeMillis();

    vertexIdToDataHandler.get(irVertex.getId()).getOutputWriters().forEach(outputWriter -> {
      outputWriter.close();
      final Optional<Long> writtenBytes = outputWriter.getWrittenBytes();
      writtenBytes.ifPresent(writtenBytesList::add);
    });

    final long writeEndTime = System.currentTimeMillis();
    metric.put("OutputWriteTime(ms)", writeEndTime - writeStartTime);
    putWrittenBytesMetric(writtenBytesList, metric);
    metricCollector.endMeasurement(irVertex.getId(), metric);
  }

  /**
   * Get input iterator from BoundedSource and bind it with id.
   */
  private void prepareInputFromSource() {
    irVertexDag.topologicalDo(irVertex -> {
      if (irVertex instanceof SourceVertex) {
        try {
          final String iteratorId = generateIteratorId();
          final Readable readable = irVertexIdToReadable.get(irVertex.getId());
          if (readable == null) {
            throw new RuntimeException(irVertex.toString());
          }
          final Iterator iterator = readable.read().iterator();
          iteratorIdToSrcIterator.putIfAbsent(iteratorId, iterator);
          srcIteratorIdToDataHandlersMap.putIfAbsent(iteratorId, new ArrayList<>());
          srcIteratorIdToDataHandlersMap.get(iteratorId).add(vertexIdToDataHandler.get(irVertex.getId()));
        } catch (final BlockFetchException ex) {
          taskStateManager.onTaskStateChanged(TaskState.State.FAILED_RECOVERABLE,
              Optional.empty(), Optional.of(TaskState.RecoverableFailureCause.INPUT_READ_FAILURE));
          LOG.error("{} Execution Failed (Recoverable: input read failure)! Exception: {}",
              taskId, ex.toString());
        } catch (final Exception e) {
          taskStateManager.onTaskStateChanged(TaskState.State.FAILED_UNRECOVERABLE,
              Optional.empty(), Optional.empty());
          LOG.error("{} Execution Failed! Exception: {}", taskId, e.toString());
          throw new RuntimeException(e);
        }
      }
    });
  }

  /**
   * Get input iterator from other stages received in the form of CompletableFuture
   * and bind it with id.
   */
  private void prepareInputFromOtherStages() {
    inputReaderToDataHandlersMap.forEach((inputReader, dataHandlers) -> {
      final List<CompletableFuture<DataUtil.IteratorWithNumBytes>> futures = inputReader.read();
      numPartitionsFromOtherStages += futures.size();

      // Add consumers which will push iterator when the futures are complete.
      futures.forEach(compFuture -> compFuture.whenComplete((iterator, exception) -> {
        if (exception != null) {
          throw new BlockFetchException(exception);
        }

        final String iteratorId = generateIteratorId();
        if (iteratorIdToDataHandlersMap.containsKey(iteratorId)) {
          throw new RuntimeException("iteratorIdToDataHandlersMap already contains " + iteratorId);
        } else {
          iteratorIdToDataHandlersMap.computeIfAbsent(iteratorId, absentIteratorId -> dataHandlers);
          try {
            vertexIdAndDataPairQueue.put(Pair.of(iteratorId, iterator));
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BlockFetchException(e);
          }
        }
      }));
    });
  }

  /**
   * Initialize the very first map of OutputCollector-children irVertex DAG.
   * In each map entry, the OutputCollector contains input data to be propagated through
   * the children irVertex DAG.
   */
  private void initializeOutputToChildrenDataHandlersMap() {
    srcIteratorIdToDataHandlersMap.values().forEach(dataHandlers ->
        dataHandlers.forEach(dataHandler -> {
          outputToChildrenDataHandlers.putIfAbsent(dataHandler.getOutputCollector(), dataHandler.getChildren());
        }));
    iteratorIdToDataHandlersMap.values().forEach(dataHandlers ->
        dataHandlers.forEach(dataHandler -> {
          outputToChildrenDataHandlers.putIfAbsent(dataHandler.getOutputCollector(), dataHandler.getChildren());
        }));
  }

  /**
   * Update the map of OutputCollector-children irVertex DAG.
   */
  private void updateOutputToChildrenDataHandlersMap() {
    Map<OutputCollectorImpl, List<DataHandler>> currentMap = outputToChildrenDataHandlers;
    Map<OutputCollectorImpl, List<DataHandler>> updatedMap = new HashMap<>();

    currentMap.values().forEach(dataHandlers ->
        dataHandlers.forEach(dataHandler -> {
          updatedMap.putIfAbsent(dataHandler.getOutputCollector(), dataHandler.getChildren());
        })
    );

    outputToChildrenDataHandlers = updatedMap;
  }

  /**
   * Update the map of OutputCollector-children irVertex DAG.
   *
   * @param irVertex the IRVertex with the transform to close.
   */
  private void closeTransform(final IRVertex irVertex) {
    if (irVertex instanceof OperatorVertex) {
      Transform transform = ((OperatorVertex) irVertex).getTransform();
      transform.close();
    }
  }

  /**
   * As a preprocessing of side input data, get inter stage side input
   * and form a map of source transform-side input.
   *
   * @param irVertex the IRVertex which receives side input from other stages.
   * @param sideInputMap the map of source transform-side input to build.
   */
  private void sideInputFromOtherStages(final IRVertex irVertex, final Map<Transform, Object> sideInputMap) {
    vertexIdToDataHandler.get(irVertex.getId()).getSideInputFromOtherStages().forEach(sideInputReader -> {
      try {
        final DataUtil.IteratorWithNumBytes sideInputIterator = sideInputReader.read().get(0).get();
        final Object sideInput = getSideInput(sideInputIterator);
        final RuntimeEdge inEdge = sideInputReader.getRuntimeEdge();
        final Transform srcTransform;
        if (inEdge instanceof StageEdge) {
          srcTransform = ((OperatorVertex) ((StageEdge) inEdge).getSrcVertex()).getTransform();
        } else {
          srcTransform = ((OperatorVertex) inEdge.getSrc()).getTransform();
        }
        sideInputMap.put(srcTransform, sideInput);

        // Collect metrics on block size if possible.
        try {
          serBlockSize += sideInputIterator.getNumSerializedBytes();
        } catch (final DataUtil.IteratorWithNumBytes.NumBytesNotSupportedException e) {
          serBlockSize = -1;
        }
        try {
          encodedBlockSize += sideInputIterator.getNumEncodedBytes();
        } catch (final DataUtil.IteratorWithNumBytes.NumBytesNotSupportedException e) {
          encodedBlockSize = -1;
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new BlockFetchException(e);
      } catch (final ExecutionException e1) {
        throw new RuntimeException("Failed while reading side input from other stages " + e1);
      }
    });
  }

  /**
   * As a preprocessing of side input data, get intra stage side input
   * and form a map of source transform-side input.
   * Assumption:  intra stage side input denotes a data element initially received
   *              via side input reader from other stages.
   *
   * @param irVertex the IRVertex which receives the data element marked as side input.
   * @param sideInputMap the map of source transform-side input to build.
   */
  private void sideInputFromThisStage(final IRVertex irVertex, final Map<Transform, Object> sideInputMap) {
    vertexIdToDataHandler.get(irVertex.getId()).getSideInputFromThisStage().forEach(input -> {
      // because sideInput is only 1 element in the outputCollector
      Object sideInput = input.remove();
      final RuntimeEdge inEdge = input.getSideInputRuntimeEdge();
      final Transform srcTransform;
      if (inEdge instanceof StageEdge) {
        srcTransform = ((OperatorVertex) ((StageEdge) inEdge).getSrcVertex()).getTransform();
      } else {
        srcTransform = ((OperatorVertex) inEdge.getSrc()).getTransform();
      }
      sideInputMap.put(srcTransform, sideInput);
    });
  }

  /**
   * Recursively process a data element down the DAG dependency.
   *
   * @param dataHandler DataHandler of a vertex to execute.
   * @param dataElement input data element to process.
   */
  private void processElementRecursively(final DataHandler dataHandler, final Object dataElement) {
    final IRVertex irVertex = dataHandler.getIRVertex();
    final OutputCollectorImpl outputCollector = dataHandler.getOutputCollector();

    // Process element-wise depending on the vertex type
    if (irVertex instanceof SourceVertex) {
      if (dataElement == null) { // null used for Beam VoidCoders
        final List<Object> nullForVoidCoder = Collections.singletonList(dataElement);
        outputCollector.emit(nullForVoidCoder);
      } else {
        outputCollector.emit(dataElement);
      }
    } else if (irVertex instanceof OperatorVertex) {
      final Transform transform = ((OperatorVertex) irVertex).getTransform();
      transform.onData(dataElement);
    } else if (irVertex instanceof MetricCollectionBarrierVertex) {
      if (dataElement == null) { // null used for Beam VoidCoders
        final List<Object> nullForVoidCoder = Collections.singletonList(dataElement);
        outputCollector.emit(nullForVoidCoder);
      } else {
        outputCollector.emit(dataElement);
      }
      setIRVertexPutOnHold((MetricCollectionBarrierVertex) irVertex);
    } else {
      throw new UnsupportedOperationException("This type of IRVertex is not supported");
    }

    // For the produced output
    while (!outputCollector.isEmpty()) {
      final Object element = outputCollector.remove();

      // Pass output to its children recursively.
      List<DataHandler> childrenDataHandlers = dataHandler.getChildren();
      if (!childrenDataHandlers.isEmpty()) {
        for (final DataHandler childDataHandler : childrenDataHandlers) {
          processElementRecursively(childDataHandler, element);
        }
      }

      // Write element-wise to OutputWriters
      dataHandler.getSinkWriters().forEach(outputWriter -> outputWriter.write(element));
    }
  }

  /**
   * Puts read bytes metric if the input data size is known.
   *
   * @param serializedBytes size in serialized (encoded and optionally post-processed (e.g. compressed)) form
   * @param encodedBytes    size in encoded form
   * @param metricMap       the metric map to put written bytes metric.
   */
  private static void putReadBytesMetric(final boolean available,
                                         final long serializedBytes,
                                         final long encodedBytes,
                                         final Map<String, Object> metricMap) {
    if (available) {
      if (serializedBytes != encodedBytes) {
        metricMap.put("ReadBytes(raw)", serializedBytes);
      }
      metricMap.put("ReadBytes", encodedBytes);
    }
  }

  /**
   * Puts written bytes metric if the output data size is known.
   *
   * @param writtenBytesList the list of written bytes.
   * @param metricMap        the metric map to put written bytes metric.
   */
  private static void putWrittenBytesMetric(final List<Long> writtenBytesList,
                                            final Map<String, Object> metricMap) {
    if (!writtenBytesList.isEmpty()) {
      long totalWrittenBytes = 0;
      for (final Long writtenBytes : writtenBytesList) {
        totalWrittenBytes += writtenBytes;
      }
      metricMap.put("WrittenBytes", totalWrittenBytes);
    }
  }

  /**
   * Get sideInput from data from {@link InputReader}.
   *
   * @param iterator data from {@link InputReader#read()}
   * @return The corresponding sideInput
   */
  private static Object getSideInput(final DataUtil.IteratorWithNumBytes iterator) {
    final List copy = new ArrayList();
    iterator.forEachRemaining(copy::add);
    if (copy.size() == 1) {
      return copy.get(0);
    } else {
      if (copy.get(0) instanceof Iterable) {
        final List collect = new ArrayList();
        copy.forEach(element -> ((Iterable) element).iterator().forEachRemaining(collect::add));
        return collect;
      } else if (copy.get(0) instanceof Map) {
        final Map collect = new HashMap();
        copy.forEach(element -> {
          final Set keySet = ((Map) element).keySet();
          keySet.forEach(key -> collect.put(key, ((Map) element).get(key)));
        });
        return collect;
      } else {
        return copy;
      }
    }
  }
}
