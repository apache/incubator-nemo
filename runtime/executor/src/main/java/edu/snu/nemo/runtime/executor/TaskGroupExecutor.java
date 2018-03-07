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
package edu.snu.nemo.runtime.executor;

import edu.snu.nemo.common.ContextImpl;
import edu.snu.nemo.common.Pair;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.exception.BlockFetchException;
import edu.snu.nemo.common.exception.BlockWriteException;
import edu.snu.nemo.common.ir.Readable;
import edu.snu.nemo.common.ir.vertex.transform.Transform;
import edu.snu.nemo.common.ir.vertex.OperatorVertex;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.plan.RuntimeEdge;
import edu.snu.nemo.runtime.common.plan.physical.*;
import edu.snu.nemo.runtime.common.state.TaskGroupState;
import edu.snu.nemo.runtime.executor.data.DataUtil;
import edu.snu.nemo.runtime.executor.datatransfer.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Executes a task group.
 */
public final class TaskGroupExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(TaskGroupExecutor.class.getName());

  private final DAG<Task, RuntimeEdge<Task>> taskGroupDag;
  private final String taskGroupId;
  private final int taskGroupIdx;
  private final TaskGroupStateManager taskGroupStateManager;
  private final List<PhysicalStageEdge> stageIncomingEdges;
  private final List<PhysicalStageEdge> stageOutgoingEdges;
  private final DataTransferFactory channelFactory;
  private final MetricCollector metricCollector;

  private final List<InputReader> inputReaders;
  private final Map<InputReader, List<Task>> inputReaderToTasksMap;
  private final Map<String, Iterator> idToSrcIteratorMap;
  private final Map<String, List<Task>> srcIteratorIdToTasksMap;
  private final Map<String, List<Task>> iteratorIdToTasksMap;
  private final LinkedBlockingQueue<Pair<String, DataUtil.IteratorWithNumBytes>> partitionQueue;
  private Map<OutputCollectorImpl, List<Task>> outputCollectorToDstTasksMap;
  private final Set<String> finishedTaskIds;
  private int numPartitions;
  private Map<String, Readable> logicalTaskIdToReadable;
  private Map<Task, TaskDataHandler> taskToDataHandlerMap;

  // For metrics
  private long serBlockSize;
  private long encodedBlockSize;

  private boolean isExecutionRequested;
  private String logicalTaskIdPutOnHold;

  private static final String ITERATORID_PREFIX = "ITERATOR_";
  private static final AtomicInteger ITERATORID_GENERATOR = new AtomicInteger(0);

  public TaskGroupExecutor(final ScheduledTaskGroup scheduledTaskGroup,
                           final DAG<Task, RuntimeEdge<Task>> taskGroupDag,
                           final TaskGroupStateManager taskGroupStateManager,
                           final DataTransferFactory channelFactory,
                           final MetricMessageSender metricMessageSender) {
    this.taskGroupDag = taskGroupDag;
    this.taskGroupId = scheduledTaskGroup.getTaskGroupId();
    this.taskGroupIdx = scheduledTaskGroup.getTaskGroupIdx();
    this.taskGroupStateManager = taskGroupStateManager;
    this.stageIncomingEdges = scheduledTaskGroup.getTaskGroupIncomingEdges();
    this.stageOutgoingEdges = scheduledTaskGroup.getTaskGroupOutgoingEdges();
    this.logicalTaskIdToReadable = scheduledTaskGroup.getLogicalTaskIdToReadable();
    this.channelFactory = channelFactory;
    this.metricCollector = new MetricCollector(metricMessageSender);
    this.logicalTaskIdPutOnHold = null;
    this.isExecutionRequested = false;

    this.inputReaders = new ArrayList<>();
    this.inputReaderToTasksMap = new ConcurrentHashMap<>();
    this.idToSrcIteratorMap = new HashMap<>();
    this.srcIteratorIdToTasksMap = new HashMap<>();
    this.iteratorIdToTasksMap = new ConcurrentHashMap<>();
    this.partitionQueue = new LinkedBlockingQueue<>();
    this.outputCollectorToDstTasksMap = new HashMap<>();
    this.taskToDataHandlerMap = new HashMap<>();

    this.finishedTaskIds = new HashSet<>();
    this.numPartitions = 0;

    this.serBlockSize = 0;
    this.encodedBlockSize = 0;


    initialize();
  }

  /**
   * Initializes readers and writers depending on the execution properties.
   * Note that there are edges that are cross-stage and stage-internal.
   */
  private void initialize() {
    // Initialize data read of SourceVertex.
    taskGroupDag.getTopologicalSort().stream()
        .filter(task -> task instanceof BoundedSourceTask)
        .forEach(boundedSourceTask -> ((BoundedSourceTask) boundedSourceTask).setReadable(
            logicalTaskIdToReadable.get(boundedSourceTask.getId())));

    // Initialize data transfer.
    taskGroupDag.topologicalDo(task -> {
      final Set<PhysicalStageEdge> inEdgesFromOtherStages = getInEdgesFromOtherStages(task);
      final Set<PhysicalStageEdge> outEdgesToOtherStages = getOutEdgesToOtherStages(task);
      taskToDataHandlerMap.putIfAbsent(task, new TaskDataHandler(task));
      final TaskDataHandler dataHandler = taskToDataHandlerMap.get(task);

      // Add InputReaders for inter-stage data transfer
      inEdgesFromOtherStages.forEach(physicalStageEdge -> {
        final InputReader inputReader = channelFactory.createReader(
            taskGroupIdx, physicalStageEdge.getSrcVertex(), physicalStageEdge);

        // For InputReaders that have side input, collect them separately.
        if (inputReader.isSideInputReader()) {
          dataHandler.addSideInputFromOtherStages(inputReader);
          LOG.info("log: {} {} Added sideInputReader (edge {})",
              taskGroupId, getPhysicalTaskId(task.getId()), physicalStageEdge.getId());
        } else {
          inputReaders.add(inputReader);
          inputReaderToTasksMap.putIfAbsent(inputReader, new ArrayList<>());
          inputReaderToTasksMap.get(inputReader).add(task);
        }
      });

      // Add OutputWriters for inter-stage data transfer
      outEdgesToOtherStages.forEach(physicalStageEdge -> {
        final OutputWriter outputWriter = channelFactory.createWriter(
            task, taskGroupIdx, physicalStageEdge.getDstVertex(), physicalStageEdge);
        dataHandler.addOutputWriters(outputWriter);
      });

      // Add InputPipes for intra-stage data transfer
      addInputFromThisStage(task);

      // Add OutputPipe for intra-stage data transfer
      setOutputCollector(task);
    });

    // Prepare Transforms if needed.
    taskGroupDag.topologicalDo(task -> {
      if (task instanceof OperatorTask) {
        final Transform transform = ((OperatorTask) task).getTransform();
        final Map<Transform, Object> sideInputMap = new HashMap<>();
        final TaskDataHandler dataHandler = taskToDataHandlerMap.get(task);
        // Check and collect side inputs.
        if (!dataHandler.getSideInputFromOtherStages().isEmpty()) {
          sideInputFromOtherStages(task, sideInputMap);
        }
        if (!dataHandler.getSideInputFromThisStage().isEmpty()) {
          sideInputFromThisStage(task, sideInputMap);
        }

        final Transform.Context transformContext = new ContextImpl(sideInputMap);
        final OutputCollectorImpl outputCollector = dataHandler.getOutputCollector();
        transform.prepare(transformContext, outputCollector);
      }
    });
  }

  // Helper functions to initializes cross-stage edges.
  private Set<PhysicalStageEdge> getInEdgesFromOtherStages(final Task task) {
    return stageIncomingEdges.stream().filter(
        stageInEdge -> stageInEdge.getDstVertex().getId().equals(task.getIrVertexId()))
        .collect(Collectors.toSet());
  }

  private Set<PhysicalStageEdge> getOutEdgesToOtherStages(final Task task) {
    return stageOutgoingEdges.stream().filter(
        stageInEdge -> stageInEdge.getSrcVertex().getId().equals(task.getIrVertexId()))
        .collect(Collectors.toSet());
  }

  /**
   * Add input OutputCollectors to each {@link Task}.
   * Input OutputCollector denotes all the OutputCollectors of intra-Stage parent tasks of this task.
   *
   * @param task the Task to add input OutputCollectors to.
   */
  private void addInputFromThisStage(final Task task) {
    final TaskDataHandler dataHandler = taskToDataHandlerMap.get(task);
    List<Task> parentTasks = taskGroupDag.getParents(task.getId());
    final String physicalTaskId = getPhysicalTaskId(task.getId());

    if (parentTasks != null) {
      parentTasks.forEach(parent -> {
        final OutputCollectorImpl parentOutputCollector = taskToDataHandlerMap.get(parent).getOutputCollector();
        if (parentOutputCollector.hasSideInputFor(physicalTaskId)) {
          dataHandler.addSideInputFromThisStage(parentOutputCollector);
        } else {
          dataHandler.addInputFromThisStages(parentOutputCollector);
          LOG.info("log: Added Output outputCollector of {} as InputPipe of {} {}",
              getPhysicalTaskId(parent.getId()), taskGroupId, physicalTaskId);
        }
      });
    }
  }

  /**
   * Add output outputCollectors to each {@link Task}.
   * Output outputCollector denotes the one and only one outputCollector of this task.
   * Check the outgoing edges that will use this outputCollector,
   * and set this outputCollector as side input if any one of the edges uses this outputCollector as side input.
   *
   * @param task the Task to add output outputCollectors to.
   */
  private void setOutputCollector(final Task task) {
    final TaskDataHandler dataHandler = taskToDataHandlerMap.get(task);
    final OutputCollectorImpl outputCollector = new OutputCollectorImpl();
    final String physicalTaskId = getPhysicalTaskId(task.getId());

    taskGroupDag.getOutgoingEdgesOf(task).forEach(outEdge -> {
      if (outEdge.isSideInput()) {
        outputCollector.setSideInputRuntimeEdge(outEdge);
        outputCollector.setAsSideInputFor(physicalTaskId);
        LOG.info("log: {} {} Marked as accepting sideInput(edge {})",
            taskGroupId, physicalTaskId, outEdge.getId());
      }
    });

    dataHandler.setOutputCollector(outputCollector);
    LOG.info("log: {} {} Added OutputPipe", taskGroupId, physicalTaskId);
  }

  private boolean hasOutputWriter(final Task task) {
    return !taskToDataHandlerMap.get(task).getOutputWriters().isEmpty();
  }

  private void setTaskPutOnHold(final MetricCollectionBarrierTask task) {
    final String physicalTaskId = getPhysicalTaskId(task.getId());
    logicalTaskIdPutOnHold = RuntimeIdGenerator.getLogicalTaskIdIdFromPhysicalTaskId(physicalTaskId);
  }

  private void writeAndCloseOutputWriters(final Task task) {
    final String physicalTaskId = getPhysicalTaskId(task.getId());
    final List<Long> writtenBytesList = new ArrayList<>();
    final Map<String, Object> metric = new HashMap<>();
    metricCollector.beginMeasurement(physicalTaskId, metric);
    final long writeStartTime = System.currentTimeMillis();

    taskToDataHandlerMap.get(task).getOutputWriters().forEach(outputWriter -> {
      LOG.info("Write and close outputWriter of task {}", getPhysicalTaskId(task.getId()));
      outputWriter.write();
      outputWriter.close();
      final Optional<Long> writtenBytes = outputWriter.getWrittenBytes();
      writtenBytes.ifPresent(writtenBytesList::add);
    });

    final long writeEndTime = System.currentTimeMillis();
    metric.put("OutputWriteTime(ms)", writeEndTime - writeStartTime);
    putWrittenBytesMetric(writtenBytesList, metric);
    metricCollector.endMeasurement(physicalTaskId, metric);
  }

  private void prepareInputFromSource() {
    taskGroupDag.topologicalDo(task -> {
      if (task instanceof BoundedSourceTask) {
        try {
          final String iteratorId = generateIteratorId();
          final Iterator iterator = ((BoundedSourceTask) task).getReadable().read().iterator();
          idToSrcIteratorMap.putIfAbsent(iteratorId, iterator);
          srcIteratorIdToTasksMap.putIfAbsent(iteratorId, new ArrayList<>());
          srcIteratorIdToTasksMap.get(iteratorId).add(task);
        } catch (final BlockFetchException ex) {
          taskGroupStateManager.onTaskGroupStateChanged(TaskGroupState.State.FAILED_RECOVERABLE,
              Optional.empty(), Optional.of(TaskGroupState.RecoverableFailureCause.INPUT_READ_FAILURE));
          LOG.info("{} Execution Failed (Recoverable: input read failure)! Exception: {}",
              taskGroupId, ex.toString());
        } catch (final Exception e) {
          taskGroupStateManager.onTaskGroupStateChanged(TaskGroupState.State.FAILED_UNRECOVERABLE,
              Optional.empty(), Optional.empty());
          LOG.info("{} Execution Failed! Exception: {}", taskGroupId, e.toString());
          throw new RuntimeException(e);
        }
      }
      // TODO #XXX: Support other types of source tasks, i. e. InitializedSourceTask
    });
  }

  private void prepareInputFromOtherStages() {
    inputReaders.stream().forEach(inputReader -> {
      final List<CompletableFuture<DataUtil.IteratorWithNumBytes>> futures = inputReader.read();
      numPartitions += futures.size();

      // Add consumers which will push iterator when the futures are complete.
      futures.forEach(compFuture -> compFuture.whenComplete((iterator, exception) -> {
        if (exception != null) {
          throw new BlockFetchException(exception);
        }

        final String iteratorId = generateIteratorId();
        if (iteratorIdToTasksMap.containsKey(iteratorId)) {
          throw new RuntimeException("iteratorIdToTasksMap already contains " + iteratorId);
        } else {
          iteratorIdToTasksMap.computeIfAbsent(iteratorId, absentIteratorId -> {
            final List<Task> list = new ArrayList<>();
            list.addAll(inputReaderToTasksMap.get(inputReader));
            return Collections.unmodifiableList(list);
          });
          try {
            partitionQueue.put(Pair.of(iteratorId, iterator));
          } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted while receiving iterator " + e);
          }
        }
      }));
    });
  }

  private boolean finishedAllTasks() {
    // Total size of this TaskGroup
    int taskNum = taskToDataHandlerMap.keySet().size();
    int finishedTaskNum = finishedTaskIds.size();

    return finishedTaskNum == taskNum;
  }

  private void initializePipeToDstTasksMap() {
    srcIteratorIdToTasksMap.values().forEach(tasks ->
        tasks.forEach(task -> {
          final OutputCollectorImpl outputCollector = taskToDataHandlerMap.get(task).getOutputCollector();
          final List<Task> dstTasks = taskGroupDag.getChildren(task.getId());
          outputCollectorToDstTasksMap.putIfAbsent(outputCollector, dstTasks);
          LOG.info("{} outputCollectorToDstTasksMap: [{}'s OutputPipe, {}]",
              taskGroupId, getPhysicalTaskId(task.getId()), dstTasks);
        }));
    iteratorIdToTasksMap.values().forEach(tasks ->
        tasks.forEach(task -> {
          final OutputCollectorImpl outputCollector = taskToDataHandlerMap.get(task).getOutputCollector();
          final List<Task> dstTasks = taskGroupDag.getChildren(task.getId());
          outputCollectorToDstTasksMap.putIfAbsent(outputCollector, dstTasks);
          LOG.info("{} outputCollectorToDstTasksMap: [{}'s OutputPipe, {}]",
              taskGroupId, getPhysicalTaskId(task.getId()), dstTasks);
        }));
  }

  private void updatePipeToDstTasksMap() {
    Map<OutputCollectorImpl, List<Task>> currentMap = outputCollectorToDstTasksMap;
    Map<OutputCollectorImpl, List<Task>> updatedMap = new HashMap<>();

    currentMap.values().forEach(tasks ->
        tasks.forEach(task -> {
          final OutputCollectorImpl outputCollector = taskToDataHandlerMap.get(task).getOutputCollector();
          final List<Task> dstTasks = taskGroupDag.getChildren(task.getId());
          updatedMap.putIfAbsent(outputCollector, dstTasks);
          LOG.info("{} outputCollectorToDstTasksMap: [{}, {}]",
              taskGroupId, getPhysicalTaskId(task.getId()), dstTasks);
        })
    );

    outputCollectorToDstTasksMap = updatedMap;
  }

  private void closeTransform(final Task task) {
    if (task instanceof OperatorTask) {
      Transform transform = ((OperatorTask) task).getTransform();
      transform.close();
      LOG.info("{} {} Closed Transform {}!", taskGroupId, getPhysicalTaskId(task.getId()), transform);
    }
  }

  private void sideInputFromOtherStages(final Task task, final Map<Transform, Object> sideInputMap) {
    taskToDataHandlerMap.get(task).getSideInputFromOtherStages().forEach(sideInputReader -> {
      try {
        final DataUtil.IteratorWithNumBytes sideInputIterator = sideInputReader.read().get(0).get();
        final Object sideInput = getSideInput(sideInputIterator);
        final RuntimeEdge inEdge = sideInputReader.getRuntimeEdge();
        final Transform srcTransform;
        if (inEdge instanceof PhysicalStageEdge) {
          srcTransform = ((OperatorVertex) ((PhysicalStageEdge) inEdge).getSrcVertex()).getTransform();
        } else {
          srcTransform = ((OperatorTask) inEdge.getSrc()).getTransform();
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

        LOG.info("log: {} {} read sideInput from InputReader {}",
            taskGroupId, getPhysicalTaskId(task.getId()), sideInput);
      } catch (final InterruptedException | ExecutionException e) {
        throw new BlockFetchException(e);
      }
    });
  }

  private void sideInputFromThisStage(final Task task, final Map<Transform, Object> sideInputMap) {
    final String physicalTaskId = getPhysicalTaskId(task.getId());
    taskToDataHandlerMap.get(task).getSideInputFromThisStage().forEach(input -> {
      // because sideInput is only 1 element in the outputCollector
      Object sideInput = input.remove();
      final RuntimeEdge inEdge = input.getSideInputRuntimeEdge();
      final Transform srcTransform;
      if (inEdge instanceof PhysicalStageEdge) {
        srcTransform = ((OperatorVertex) ((PhysicalStageEdge) inEdge).getSrcVertex()).getTransform();
      } else {
        srcTransform = ((OperatorTask) inEdge.getSrc()).getTransform();
      }
      sideInputMap.put(srcTransform, sideInput);
      LOG.info("log: {} {} read sideInput from InputPipe {}", taskGroupId, physicalTaskId, sideInput);
    });
  }

  /**
   * Executes the task group.
   */
  public void execute() {
    final Map<String, Object> metric = new HashMap<>();
    metricCollector.beginMeasurement(taskGroupId, metric);
    long boundedSrcReadStartTime = 0;
    long boundedSrcReadEndTime = 0;
    long inputReadStartTime = 0;
    long inputReadEndTime = 0;
    if (isExecutionRequested) {
      throw new RuntimeException("TaskGroup {" + taskGroupId + "} execution called again!");
    } else {
      isExecutionRequested = true;
    }
    taskGroupStateManager.onTaskGroupStateChanged(TaskGroupState.State.EXECUTING, Optional.empty(), Optional.empty());
    LOG.info("{} Executing!", taskGroupId);

    // Prepare input data from bounded source.
    boundedSrcReadStartTime = System.currentTimeMillis();
    prepareInputFromSource();
    boundedSrcReadEndTime = System.currentTimeMillis();
    metric.put("BoundedSourceReadTime(ms)", boundedSrcReadEndTime - boundedSrcReadStartTime);

    // Prepare input data from other stages.
    inputReadStartTime = System.currentTimeMillis();
    prepareInputFromOtherStages();

    // Execute the TaskGroup DAG.
    try {
      srcIteratorIdToTasksMap.forEach((srcIteratorId, tasks) -> {
        Iterator iterator = idToSrcIteratorMap.get(srcIteratorId);
        iterator.forEachRemaining(element -> {
          for (final Task task : tasks) {
            List data = Collections.singletonList(element);
            runTask(task, data);
          }
        });
      });

      // Process data from other stages.
      for (int currPartition = 0; currPartition < numPartitions; currPartition++) {
        LOG.info("{} Partition {} out of {}", taskGroupId, currPartition, numPartitions);

        Pair<String, DataUtil.IteratorWithNumBytes> idToIteratorPair = partitionQueue.take();
        final String iteratorId = idToIteratorPair.left();
        final DataUtil.IteratorWithNumBytes iterator = idToIteratorPair.right();
        List<Task> dstTasks = iteratorIdToTasksMap.get(iteratorId);
        iterator.forEachRemaining(element -> {
          for (final Task task : dstTasks) {
            List data = Collections.singletonList(element);
            runTask(task, data);
          }
        });

        // Collect metrics on block size if possible.
        try {
          serBlockSize += iterator.getNumSerializedBytes();
        } catch (final DataUtil.IteratorWithNumBytes.NumBytesNotSupportedException e) {
          serBlockSize = -1;
        } catch (final IllegalStateException e) {
          LOG.error("IllegalState ", e);
        }
        try {
          encodedBlockSize += iterator.getNumEncodedBytes();
        } catch (final DataUtil.IteratorWithNumBytes.NumBytesNotSupportedException e) {
          encodedBlockSize = -1;
        } catch (final IllegalStateException e) {
          LOG.error("IllegalState ", e);
        }
      }
      inputReadEndTime = System.currentTimeMillis();
      metric.put("InputReadTime(ms)", inputReadEndTime - inputReadStartTime);
      LOG.info("{} Finished processing src data!", taskGroupId);

      // Process intra-TaskGroup data.
      // Intra-TaskGroup data comes from outputCollectors of this TaskGroup's Tasks.
      initializePipeToDstTasksMap();
      while (!finishedAllTasks()) {
        outputCollectorToDstTasksMap.forEach((outputCollector, dstTasks) -> {
          // Get the task that has this outputCollector as its output outputCollector
          Task outputCollectorOwnerTask = taskToDataHandlerMap.values().stream()
              .filter(dataHandler -> dataHandler.getOutputCollector() == outputCollector)
              .findFirst().get().getTask();
          LOG.info("{} outputCollectorOwnerTask {}", taskGroupId, getPhysicalTaskId(outputCollectorOwnerTask.getId()));

          // Before consuming the output of outputCollectorOwnerTask as input,
          // close transform if it is OperatorTransform.
          closeTransform(outputCollectorOwnerTask);

          // Set outputCollectorOwnerTask as finished.
          finishedTaskIds.add(getPhysicalTaskId(outputCollectorOwnerTask.getId()));

          // Pass outputCollectorOwnerTask's output to its children tasks recursively.
          if (!dstTasks.isEmpty()) {
            while (!outputCollector.isEmpty()) {
              // Form input element-wise from the outputCollector
              final List input = Collections.singletonList(outputCollector.remove());
              for (final Task task : dstTasks) {
                runTask(task, input);
              }
            }
          }

          // Write the whole iterable and close the OutputWriters.
          if (hasOutputWriter(outputCollectorOwnerTask)) {
            // If outputCollector isn't empty(if closeTransform produced some output),
            // write them element-wise to OutputWriters.
            while (!outputCollector.isEmpty()) {
              final Object element = outputCollector.remove();
              List<OutputWriter> outputWritersOfTask =
                  taskToDataHandlerMap.get(outputCollectorOwnerTask).getOutputWriters();
              outputWritersOfTask.forEach(outputWriter -> outputWriter.writeElement(element));
              LOG.info("{} {} Write to OutputWriter element {}",
                  taskGroupId, getPhysicalTaskId(outputCollectorOwnerTask.getId()), element);
            }
            writeAndCloseOutputWriters(outputCollectorOwnerTask);
          }
        });
        updatePipeToDstTasksMap();
      }
    } catch (final BlockWriteException ex2) {
      taskGroupStateManager.onTaskGroupStateChanged(TaskGroupState.State.FAILED_RECOVERABLE,
          Optional.empty(), Optional.of(TaskGroupState.RecoverableFailureCause.OUTPUT_WRITE_FAILURE));
      LOG.info("{} Execution Failed (Recoverable: output write failure)! Exception: {}",
          taskGroupId, ex2.toString());
    } catch (final Exception e) {
      taskGroupStateManager.onTaskGroupStateChanged(TaskGroupState.State.FAILED_UNRECOVERABLE,
          Optional.empty(), Optional.empty());
      LOG.info("{} Execution Failed! Exception: {}",
          taskGroupId, e.toString());
      throw new RuntimeException(e);
    }

    // Put TaskGroup-unit metrics.
    final boolean available = serBlockSize >= 0;
    putReadBytesMetric(available, serBlockSize, encodedBlockSize, metric);
    metricCollector.endMeasurement(taskGroupId, metric);
    if (logicalTaskIdPutOnHold == null) {
      taskGroupStateManager.onTaskGroupStateChanged(TaskGroupState.State.COMPLETE, Optional.empty(), Optional.empty());
    } else {
      taskGroupStateManager.onTaskGroupStateChanged(TaskGroupState.State.ON_HOLD,
          Optional.of(logicalTaskIdPutOnHold),
          Optional.empty());
    }
    LOG.info("{} Complete!", taskGroupId);
  }

  /**
   * Processes an OperatorTask.
   *
   * @param task to execute
   */
  private void runTask(final Task task, final List<Object> data) {
    final String physicalTaskId = getPhysicalTaskId(task.getId());

    // Process element-wise depending on the Task type
    if (task instanceof BoundedSourceTask) {
      OutputCollectorImpl outputCollector = taskToDataHandlerMap.get(task).getOutputCollector();

      if (data.contains(null)) {  // data is [null] used for VoidCoders
        outputCollector.emit(data);
      } else {
        data.forEach(dataElement -> {
          outputCollector.emit(dataElement);
          LOG.info("log: {} {} BoundedSourceTask emitting {} to outputCollector",
              taskGroupId, physicalTaskId, dataElement);
        });
      }
    } else if (task instanceof OperatorTask) {
      final Transform transform = ((OperatorTask) task).getTransform();

      // Consumes the received element from incoming edges.
      // Calculate the number of inter-TaskGroup data to process.
      int numElements = data.size();
      LOG.info("log: {} {}: numElements {}", taskGroupId, physicalTaskId, numElements);

      IntStream.range(0, numElements).forEach(dataNum -> {
        Object dataElement = data.get(dataNum);
        LOG.info("log: {} {} OperatorTask applying {} to onData", taskGroupId, physicalTaskId, dataElement);
        transform.onData(dataElement);
      });
    } else if (task instanceof MetricCollectionBarrierTask) {
      OutputCollectorImpl outputCollector = taskToDataHandlerMap.get(task).getOutputCollector();

      if (data.contains(null)) {  // data is [null] used for VoidCoders
        outputCollector.emit(data);
      } else {
        data.forEach(dataElement -> {
          outputCollector.emit(dataElement);
          LOG.info("log: {} {} MetricCollectionTask emitting {} to outputCollector",
              taskGroupId, physicalTaskId, dataElement);
        });
      }
      setTaskPutOnHold((MetricCollectionBarrierTask) task);
    } else {
      throw new UnsupportedOperationException("This type  of Task is not supported");
    }

    // For the produced output
    OutputCollectorImpl outputCollector = taskToDataHandlerMap.get(task).getOutputCollector();
    while (!outputCollector.isEmpty()) {
      final Object element = outputCollector.remove();

      // Write element-wise to OutputWriters if any
      if (hasOutputWriter(task)) {
        List<OutputWriter> outputWritersOfTask = taskToDataHandlerMap.get(task).getOutputWriters();
        outputWritersOfTask.forEach(outputWriter -> outputWriter.writeElement(element));
        LOG.info("{} {} Write to OutputWriter element {}",
            taskGroupId, getPhysicalTaskId(task.getId()), element);
      }

      // Pass output to its children tasks recursively.
      List<Task> dstTasks = taskGroupDag.getChildren(task.getId());
      if (!dstTasks.isEmpty()) {
        final List output = Collections.singletonList(element); // intra-TaskGroup data are safe here
        for (final Task dstTask : dstTasks) {
          LOG.info("{} {} input to runTask {}", taskGroupId, getPhysicalTaskId(dstTask.getId()), output);
          runTask(dstTask, output);
        }
      }
    }
  }

  /**
   * @param logicalTaskId the logical task id.
   * @return the physical task id.
   */
  private String getPhysicalTaskId(final String logicalTaskId) {
    return RuntimeIdGenerator.generatePhysicalTaskId(taskGroupIdx, logicalTaskId);
  }

  private String generateIteratorId() {
    return ITERATORID_PREFIX + ITERATORID_GENERATOR.getAndIncrement();
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
