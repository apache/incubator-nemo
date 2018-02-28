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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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

  // List of this TaskGroup's readers/writers that deals with inter-TaskGroup data.
  private final List<InputReader> inputReaders;
  private final Map<InputReader, List<Task>> inputReaderToTasksMap;
  // Map of task ID to its intra-TaskGroup data pipe.
  private final Map<Task, List<PipeImpl>> taskToInputPipesMap;
  private final Map<Task, PipeImpl> taskToOutputPipeMap;  // one and only one Pipe per task
  private final Map<Task, List<InputReader>> taskToInputReadersMap;
  private final Map<Task, List<InputReader>> taskToSideInputReadersMap;
  private final Map<Task, List<OutputWriter>> taskToOutputWritersMap;
  private final Map<String, List<Task>> iteratorIdToTasksMap;
  private final Map<String, Iterator> idToIteratorMap;
  private volatile Map<PipeImpl, List<Task>> pipeToDstTasksMap;
  private volatile boolean isExecutionRequested;
  private volatile String logicalTaskIdPutOnHold;
  private volatile int numIterators;
  private final Set<Transform> preparedTransforms;
  private final Set<String> finishedTaskIds;

  private static final String ITERATOR_PREFIX = "Iterator-";
  private static final AtomicInteger ITERATORID_GENERATOR = new AtomicInteger(0);

  private final AtomicInteger completedFutures;
  private int numBoundedSources;

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
    this.channelFactory = channelFactory;
    this.metricCollector = new MetricCollector(metricMessageSender);

    this.inputReaders = new ArrayList<>();
    this.taskToInputPipesMap = new HashMap<>();
    this.taskToOutputPipeMap = new HashMap<>();
    this.taskToInputReadersMap = new HashMap<>();
    this.taskToOutputWritersMap = new HashMap<>();
    this.taskToSideInputReadersMap = new HashMap<>();
    this.inputReaderToTasksMap = new ConcurrentHashMap<>();
    this.iteratorIdToTasksMap = new ConcurrentHashMap<>();
    this.idToIteratorMap = new ConcurrentHashMap<>();
    this.pipeToDstTasksMap = new HashMap<>();

    this.logicalTaskIdPutOnHold = null;
    this.isExecutionRequested = false;
    this.preparedTransforms = new HashSet<>();
    this.finishedTaskIds = new HashSet<>();
    this.numIterators = 0;
    this.completedFutures = new AtomicInteger(0);
    this.numBoundedSources = 0;

    initializeDataRead(scheduledTaskGroup.getLogicalTaskIdToReadable());
    initializeDataTransfer();
  }

  /**
   * Initializes data read of {@link edu.snu.nemo.common.ir.vertex.SourceVertex}.
   *
   * @param logicalTaskIdToReadable the map between logical task id to {@link Readable}.
   */
  private void initializeDataRead(final Map<String, Readable> logicalTaskIdToReadable) {
    taskGroupDag.getTopologicalSort().stream()
        .filter(task -> task instanceof BoundedSourceTask)
        .forEach(boundedSourceTask -> ((BoundedSourceTask) boundedSourceTask).setReadable(
            logicalTaskIdToReadable.get(boundedSourceTask.getId())));
  }

  /**
   * Initializes readers and writers depending on the execution properties.
   * Note that there are edges that are cross-stage and stage-internal.
   */
  private void initializeDataTransfer() {
    taskGroupDag.topologicalDo(task -> {
      final Set<PhysicalStageEdge> inEdgesFromOtherStages = getInEdgesFromOtherStages(task);
      final Set<PhysicalStageEdge> outEdgesToOtherStages = getOutEdgesToOtherStages(task);

      // Add InputReaders for inter-stage data transfer
      inEdgesFromOtherStages.forEach(physicalStageEdge -> {
        final InputReader inputReader = channelFactory.createReader(
            taskGroupIdx, physicalStageEdge.getSrcVertex(), physicalStageEdge);

        // For InputReaders that have side input, collect them separately.
        if (inputReader.isSideInputReader()) {
          taskToSideInputReadersMap.putIfAbsent(task, new ArrayList<>());
          taskToSideInputReadersMap.get(task).add(inputReader);
          LOG.info("log: {} {} Added sideInputReader (edge {})",
              taskGroupId, getPhysicalTaskId(task.getId()), physicalStageEdge.getId());
        } else {
          inputReaders.add(inputReader);
          taskToInputReadersMap.putIfAbsent(task, new ArrayList<>());
          taskToInputReadersMap.get(task).add(inputReader);

          inputReaderToTasksMap.putIfAbsent(inputReader, new ArrayList<>());
          inputReaderToTasksMap.get(inputReader).add(task);
        }
      });

      // Add OutputWriters for inter-stage data transfer
      outEdgesToOtherStages.forEach(physicalStageEdge -> {
        final OutputWriter outputWriter = channelFactory.createWriter(
            task, taskGroupIdx, physicalStageEdge.getDstVertex(), physicalStageEdge);
        taskToOutputWritersMap.putIfAbsent(task, new ArrayList<>());
        taskToOutputWritersMap.get(task).add(outputWriter);
      });

      // Add InputPipes for intra-stage data transfer
      addInputPipe(task);

      // Add OutputPipe for intra-stage data transfer
      addOutputPipe(task);
    });
    LOG.info("{} InputReaders: {}", taskGroupId, inputReaders);
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
   * Add input pipes to each {@link Task}.
   * Input pipe denotes all the pipes of intra-Stage parent tasks of this task.
   *
   * @param task the Task to add input pipes to.
   */
  private void addInputPipe(final Task task) {
    List<PipeImpl> inputPipes = new ArrayList<>();
    List<Task> parentTasks = taskGroupDag.getParents(task.getId());
    final String physicalTaskId = getPhysicalTaskId(task.getId());

    if (parentTasks != null) {
      parentTasks.forEach(parent -> {
        final PipeImpl parentOutputPipe = taskToOutputPipeMap.get(parent);
        inputPipes.add(parentOutputPipe);
        LOG.info("log: Added Outputpipe of {} as InputPipe of {} {}",
            getPhysicalTaskId(parent.getId()), taskGroupId, physicalTaskId);
      });
      taskToInputPipesMap.put(task, inputPipes);
    }
  }

  /**
   * Add output pipes to each {@link Task}.
   * Output pipe denotes the one and only one pipe of this task.
   * Check the outgoing edges that will use this pipe,
   * and set this pipe as side input if any one of the edges uses this pipe as side input.
   *
   * @param task the Task to add output pipes to.
   */
  private void addOutputPipe(final Task task) {
    final PipeImpl outputPipe = new PipeImpl();
    final String physicalTaskId = getPhysicalTaskId(task.getId());
    final List<RuntimeEdge<Task>> outEdges = taskGroupDag.getOutgoingEdgesOf(task);

    outEdges.forEach(outEdge -> {
      if (outEdge.isSideInput()) {
        outputPipe.setSideInputRuntimeEdge(outEdge);
        outputPipe.setAsSideInput(physicalTaskId);
        LOG.info("log: {} {} Marked as accepting sideInput(edge {})",
            taskGroupId, physicalTaskId, outEdge.getId());
      }
    });

    taskToOutputPipeMap.put(task, outputPipe);
    LOG.info("log: {} {} Added OutputPipe", taskGroupId, physicalTaskId);
  }

  private boolean hasInputPipe(final Task task) {
    return taskToInputPipesMap.containsKey(task);
  }

  private boolean hasOutputWriter(final Task task) {
    return taskToOutputWritersMap.containsKey(task);
  }

  private void setTaskPutOnHold(final MetricCollectionBarrierTask task) {
    final String physicalTaskId = getPhysicalTaskId(task.getId());
    logicalTaskIdPutOnHold = RuntimeIdGenerator.getLogicalTaskIdIdFromPhysicalTaskId(physicalTaskId);
  }

  private void writeAndCloseOutputWriters(final Task task) {
    taskToOutputWritersMap.get(task).forEach(outputWriter -> {
      LOG.info("Write and close outputWriter of task {}", getPhysicalTaskId(task.getId()));
      outputWriter.write();
      outputWriter.close();
    });
  }

  private void prepareInputFromSource() {
    taskGroupDag.topologicalDo(task -> {
      if (task instanceof BoundedSourceTask) {
        try {
          final Readable readable = ((BoundedSourceTask) task).getReadable();
          final Iterable readData = readable.read();
          numIterators++;  // Aggregate total number of partitions to process
          numBoundedSources++;
          final String iteratorId = generateIteratorId();
          idToIteratorMap.putIfAbsent(iteratorId, readData.iterator());
          iteratorIdToTasksMap.putIfAbsent(iteratorId, new ArrayList<>());
          iteratorIdToTasksMap.get(iteratorId).add(task);
          LOG.info("iteratorId : Tasks {}", iteratorIdToTasksMap);
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
      final List<CompletableFuture<Iterator>> futures = inputReader.read();
      numIterators += futures.size();  // Aggregate total number of partitions to process.
      // Add consumers which will push the data to the iterators when it ready to the futures.
      futures.forEach(compFuture -> compFuture.whenComplete((iterator, exception) -> {
        if (exception != null) {
          LOG.info("{} failed in prepareInputFromOtherSource while fetching blocks", taskGroupId);
          throw new BlockFetchException(exception);
        }

        completedFutures.getAndIncrement();

        final String iteratorId = generateIteratorId();
        idToIteratorMap.putIfAbsent(iteratorId, iterator);
        iteratorIdToTasksMap.computeIfAbsent(iteratorId, absentIteratorId -> {
          final List<Task> list = new CopyOnWriteArrayList<>(); //new ArrayList<>();
          list.addAll(inputReaderToTasksMap.get(inputReader));
          return Collections.unmodifiableList(list);
        });
      }));
    });
  }

  private boolean finishedSrcData() {
    int numInterStageData = completedFutures.get() + numBoundedSources;
    boolean receivedAll = (numInterStageData == numIterators);
    boolean setAll = (numInterStageData == idToIteratorMap.entrySet().size());

    LOG.info("{} numIterators {} numInterStageData {}, itrMap size {}",
        taskGroupId, numIterators, numInterStageData, idToIteratorMap.entrySet().size());

    if (!(receivedAll && setAll)) {
      return false;
    }

    boolean finishedAll = true;
    for (Map.Entry<String, Iterator> entry : idToIteratorMap.entrySet()) {
      Iterator iterator = entry.getValue();
      if (iterator.hasNext()) {
        finishedAll = false;
      }
    }

    return receivedAll && finishedAll;
  }

  private boolean finishedAllTasks() {
    // Total size of this TaskGroup
    int taskNum = taskToOutputPipeMap.keySet().size();
    int finishedTaskNum = finishedTaskIds.size();

    LOG.info("{} Total {} Finished {}", taskGroupId, taskNum, finishedTaskNum);

    return finishedTaskNum == taskNum;
  }

  private void initializePipeToDstTasksMap() {
    iteratorIdToTasksMap.values().forEach(tasks ->
        tasks.stream().forEach(task -> {
          final List<Task> dstTasks = taskGroupDag.getChildren(task.getId());
          PipeImpl pipe = taskToOutputPipeMap.get(task);
          pipeToDstTasksMap.putIfAbsent(pipe, dstTasks);
          LOG.info("{} pipeToDstTasksMap: [{}'s OutputPipe, {}]",
              taskGroupId, getPhysicalTaskId(task.getId()), dstTasks);
        })
    );
  }

  private void updatePipeToDstTasksMap() {
    Map<PipeImpl, List<Task>> currentMap = pipeToDstTasksMap;
    Map<PipeImpl, List<Task>> updatedMap = new HashMap<>();

    currentMap.values().forEach(tasks ->
        tasks.stream().forEach(task -> {
          final List<Task> dstTasks = taskGroupDag.getChildren(task.getId());
          PipeImpl pipe = taskToOutputPipeMap.get(task);
          updatedMap.putIfAbsent(pipe, dstTasks);
          LOG.info("{} pipeToDstTasksMap: [{}, {}]",
              taskGroupId, getPhysicalTaskId(task.getId()), dstTasks);
        })
    );

    pipeToDstTasksMap = updatedMap;
  }

  private void closeTransform(final Task task) {
    if (task instanceof OperatorTask) {
      Transform transform = ((OperatorTask) task).getTransform();
      transform.close();
      LOG.info("{} {} Closed Transform {}!", taskGroupId, getPhysicalTaskId(task.getId()), transform);
    }
  }

  private Map<Transform, Object> sideInputFromOtherStages(final Task task, final Map<Transform, Object> sideInputMap) {
    taskToSideInputReadersMap.get(task).forEach(sideInputReader -> {
      try {
        final Object sideInput = sideInputReader.getSideInput();
        final RuntimeEdge inEdge = sideInputReader.getRuntimeEdge();
        final Transform srcTransform;
        if (inEdge instanceof PhysicalStageEdge) {
          srcTransform = ((OperatorVertex) ((PhysicalStageEdge) inEdge).getSrcVertex()).getTransform();
        } else {
          srcTransform = ((OperatorTask) inEdge.getSrc()).getTransform();
        }
        sideInputMap.put(srcTransform, sideInput);
        LOG.info("log: {} {} read sideInput from InputReader {}",
            taskGroupId, getPhysicalTaskId(task.getId()), sideInput);
      } catch (final InterruptedException | ExecutionException e) {
        throw new BlockFetchException(e);
      }
    });

    return sideInputMap;
  }

  private Map<Transform, Object> sideInputFromThisStage(final Task task, final Map<Transform, Object> sideInputMap) {
    final String physicalTaskId = getPhysicalTaskId(task.getId());

    taskToInputPipesMap.get(task).stream().forEach(inputPipe -> {
      if (inputPipe.hasSideInputFor(physicalTaskId)) {
        // because sideInput is only 1 element in the pipe
        Object sideInput = inputPipe.remove();
        final RuntimeEdge inEdge = inputPipe.getSideInputRuntimeEdge();
        final Transform srcTransform;
        if (inEdge instanceof PhysicalStageEdge) {
          srcTransform = ((OperatorVertex) ((PhysicalStageEdge) inEdge).getSrcVertex()).getTransform();
        } else {
          srcTransform = ((OperatorTask) inEdge.getSrc()).getTransform();
        }
        sideInputMap.put(srcTransform, sideInput);
        LOG.info("log: {} {} read sideInput from InputPipe {}", taskGroupId, physicalTaskId, sideInput);
      }
    });

    return sideInputMap;
  }

  private void prepareTransform(final Transform transform, final Task task) {
    if (!preparedTransforms.contains(transform)) {
      final Map<Transform, Object> sideInputMap = new HashMap<>();

      // Check and collect side inputs.
      if (taskToSideInputReadersMap.keySet().contains(task)) {
        sideInputFromOtherStages(task, sideInputMap);
      }
      if (hasInputPipe(task)) {
        sideInputFromThisStage(task, sideInputMap);
      }

      final Transform.Context transformContext = new ContextImpl(sideInputMap);
      final PipeImpl outputPipe = taskToOutputPipeMap.get(task);
      transform.prepare(transformContext, outputPipe);

      preparedTransforms.add(transform);
    }
  }

  /**
   * Executes the task group.
   */
  public void execute() {
    if (isExecutionRequested) {
      throw new RuntimeException("TaskGroup {" + taskGroupId + "} execution called again!");
    } else {
      isExecutionRequested = true;
    }

    taskGroupStateManager.onTaskGroupStateChanged(TaskGroupState.State.EXECUTING, Optional.empty(), Optional.empty());
    LOG.info("{} Executing!", taskGroupId);

    // Prepare input data from bounded source.
    prepareInputFromSource();

    // Prepare input data from other stages.
    prepareInputFromOtherStages();

    // Execute the TaskGroup DAG.
    try {
      // Process source data.
      // Source data comes from SourceTasks and other stages.
      while (!finishedSrcData()) {
        iteratorIdToTasksMap.forEach((iteratorId, tasks) -> {
              Iterator iterator = idToIteratorMap.get(iteratorId);
              iterator.forEachRemaining(element -> {
                for (final Task task : tasks) {
                  List data = Collections.singletonList(element);
                  LOG.info("{} {} read from {}(total {} iterators): {}",
                      taskGroupId, getPhysicalTaskId(task.getId()), iteratorId, numIterators, data);
                  runTask(task, data);
                }
              });
            }
      }

      LOG.info("{} Finished processing src data!", taskGroupId);

      // Process intra-TaskGroup data.
      // Intra-TaskGroup data comes from pipes of this TaskGroup's Tasks.
      initializePipeToDstTasksMap();
      while (!finishedAllTasks()) {
        pipeToDstTasksMap.forEach((pipe, dstTasks) -> {
          // Get the task that has this pipe as its output pipe
          Task pipeOwnerTask = taskToOutputPipeMap.entrySet().stream()
              .filter(entry -> entry.getValue().equals(pipe))
              .findAny().get().getKey();
          LOG.info("{} pipeOwnerTask {}", taskGroupId, getPhysicalTaskId(pipeOwnerTask.getId()));

          // Before consuming the output of pipeOwnerTask as input,
          // close transform if it is OperatorTransform.
          closeTransform(pipeOwnerTask);

          // Set pipeOwnerTask as finished.
          finishedTaskIds.add(getPhysicalTaskId(pipeOwnerTask.getId()));
          LOG.info("{} {} Finished!", taskGroupId, getPhysicalTaskId(pipeOwnerTask.getId()));

          // Pass pipeOwnerTask's output to its children tasks recursively.
          if (!dstTasks.isEmpty()) {
            while (!pipe.isEmpty()) {
              // Form input element-wise from the pipe
              final List input = Collections.singletonList(pipe.remove());
              for (final Task task : dstTasks) {
                runTask(task, input);
              }
            }
          }

          // Write the whole iterable and close the OutputWriters.
          if (hasOutputWriter(pipeOwnerTask)) {
            // If pipe isn't empty(if closeTransform produced some output),
            // write them element-wise to OutputWriters.
            while (!pipe.isEmpty()) {
              final Object element = pipe.remove();
              List<OutputWriter> outputWritersOfTask = taskToOutputWritersMap.get(pipeOwnerTask);
              outputWritersOfTask.forEach(outputWriter -> outputWriter.writeElement(element));
              LOG.info("{} {} Write to OutputWriter element {}",
                  taskGroupId, getPhysicalTaskId(pipeOwnerTask.getId()), element);
            }
            writeAndCloseOutputWriters(pipeOwnerTask);
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
      PipeImpl pipe = taskToOutputPipeMap.get(task);

      if (data.contains(null)) {  // data is [null] used for VoidCoders
        pipe.emit(data);
      } else {
        data.forEach(dataElement -> {
          pipe.emit(dataElement);
          LOG.info("log: {} {} BoundedSourceTask emitting {} to pipe",
              taskGroupId, physicalTaskId, dataElement);
        });
      }
    } else if (task instanceof OperatorTask) {
      final Transform transform = ((OperatorTask) task).getTransform();
      prepareTransform(transform, task);

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
      PipeImpl pipe = taskToOutputPipeMap.get(task);

      if (data.contains(null)) {  // data is [null] used for VoidCoders
        pipe.emit(data);
      } else {
        data.forEach(dataElement -> {
          pipe.emit(dataElement);
          LOG.info("log: {} {} MetricCollectionTask emitting {} to pipe",
              taskGroupId, physicalTaskId, dataElement);
        });
      }
      setTaskPutOnHold((MetricCollectionBarrierTask) task);
    }

    // For the produced output
    PipeImpl pipe = taskToOutputPipeMap.get(task);
    while (!pipe.isEmpty()) {
      final Object element = pipe.remove();

      // Write element-wise to OutputWriters if any
      if (hasOutputWriter(task)) {
        List<OutputWriter> outputWritersOfTask = taskToOutputWritersMap.get(task);
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
                final String itrID = ITERATOR_PREFIX + ITERATORID_GENERATOR.getAndIncrement();
                LOG.info("{} generateIteratorId {}", taskGroupId, itrID);
                return itrID;
              }
  /**
   * Puts read bytes metric if the input data size is known.
   *
   * @param available whether input data size is known or not
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
