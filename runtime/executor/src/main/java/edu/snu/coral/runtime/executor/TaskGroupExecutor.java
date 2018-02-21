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
package edu.snu.coral.runtime.executor;

import edu.snu.coral.common.ContextImpl;
import edu.snu.coral.common.dag.DAG;
import edu.snu.coral.common.exception.BlockFetchException;
import edu.snu.coral.common.exception.BlockWriteException;
import edu.snu.coral.common.ir.Pipe;
import edu.snu.coral.common.ir.Readable;
import edu.snu.coral.common.ir.vertex.transform.Transform;
import edu.snu.coral.common.ir.vertex.OperatorVertex;
import edu.snu.coral.runtime.common.RuntimeIdGenerator;
import edu.snu.coral.runtime.common.plan.RuntimeEdge;
import edu.snu.coral.runtime.common.plan.physical.*;
import edu.snu.coral.runtime.common.state.TaskGroupState;
import edu.snu.coral.runtime.executor.datatransfer.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

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

  // List of this TaskGroup's readers/writers that deals with inter-TaskGroup data.
  private final List<InputReader> inputReaders;
  private final List<OutputWriter> outputWriters;
  private final Map<InputReader, List<Task>> inputReaderToTasksMap;
  // Map of task ID to its intra-TaskGroup data pipe.
  private final Map<String, List<PipeImpl>> physicalTaskIdToInputPipesMap;
  private final Map<String, PipeImpl> physicalTaskIdToOutputPipeMap;  // one and only one Pipe per task
  private final List<Task> sourceTasks;
  private final List<Task> sinkTasks;
  private final Map<Task, List<InputReader>> srcTaskToSideInputReadersMap;
  private final Map<String, List<Task>> iteratorIdToTasksMap;
  private final Map<String, Iterator> idToIteratorMap;
  private Map<PipeImpl, List<Task>> pipeToDstTasksMap;
  private boolean isExecutionRequested;
  private String logicalTaskIdPutOnHold;
  private final Set<Transform> preparedTransforms;
  private int numPartitions;
  private boolean processedSrcData;

  private static final String ITERATOR_PREFIX = "Iterator-";
  private static AtomicInteger iteratorIdGenerator;

  public TaskGroupExecutor(final ScheduledTaskGroup scheduledTaskGroup,
                           final DAG<Task, RuntimeEdge<Task>> taskGroupDag,
                           final TaskGroupStateManager taskGroupStateManager,
                           final DataTransferFactory channelFactory) {
    this.taskGroupDag = taskGroupDag;
    this.taskGroupId = scheduledTaskGroup.getTaskGroupId();
    this.taskGroupIdx = scheduledTaskGroup.getTaskGroupIdx();
    this.taskGroupStateManager = taskGroupStateManager;
    this.stageIncomingEdges = scheduledTaskGroup.getTaskGroupIncomingEdges();
    this.stageOutgoingEdges = scheduledTaskGroup.getTaskGroupOutgoingEdges();
    this.channelFactory = channelFactory;

    this.inputReaders = new ArrayList<>();
    this.outputWriters = new ArrayList<>();
    this.idToIteratorMap = new HashMap<>();
    this.inputReaderToTasksMap = new HashMap<>();
    this.physicalTaskIdToInputPipesMap = new HashMap<>();
    this.physicalTaskIdToOutputPipeMap = new HashMap<>();
    this.sourceTasks = new ArrayList<>();
    this.sinkTasks = new ArrayList<>();
    this.srcTaskToSideInputReadersMap = new HashMap<>();
    this.iteratorIdToTasksMap = new HashMap<>();
    this.pipeToDstTasksMap = new HashMap<>();

    this.logicalTaskIdPutOnHold = null;
    this.isExecutionRequested = false;
    this.preparedTransforms = new HashSet<>();
    this.numPartitions = 0;
    this.processedSrcData = false;

    iteratorIdGenerator = new AtomicInteger(0);

    initializeDataRead(scheduledTaskGroup.getLogicalTaskIdToReadable());
    initializeDataTransfer();
  }

  /**
   * Initializes data read of {@link edu.snu.coral.common.ir.vertex.SourceVertex}.
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
    setSourceTasks();
    setSinkTasks();

    taskGroupDag.topologicalDo(task -> {
      final Set<PhysicalStageEdge> inEdgesFromOtherStages = getInEdgesFromOtherStages(task);
      final Set<PhysicalStageEdge> outEdgesToOtherStages = getOutEdgesToOtherStages(task);

      // Add InputReaders for inter-stage data transfer
      inEdgesFromOtherStages.forEach(physicalStageEdge -> {
        final InputReader inputReader = channelFactory.createReader(
            taskGroupIdx, physicalStageEdge.getSrcVertex(), physicalStageEdge);

        // If InputReader that has side input, collect them separately.
        if (inputReader.isSideInputReader()) {
          srcTaskToSideInputReadersMap.putIfAbsent(task, new ArrayList<>());
          srcTaskToSideInputReadersMap.get(task).add(inputReader);
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
        outputWriters.add(outputWriter);
      });

      // Add InputPipes for intra-stage data transfer
      addInputPipe(task);

      // Add OutputPipe for intra-stage data transfer
      addOutputPipe(task);
    });
    LOG.info("{} InputReaders: {}", taskGroupId, inputReaders);
    LOG.info("{} OutputWriters: {}", taskGroupId, outputWriters);
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

  private void setSourceTasks() {
    taskGroupDag.topologicalDo(task -> {
      // TODO #XXX: Count other types of source tasks, i. e. InitializedSourceTask
      if (task instanceof BoundedSourceTask
          || getInEdgesFromOtherStages(task).size() > 0) {
        sourceTasks.add(task);
        LOG.info("Source Task: {} {}", taskGroupId, getPhysicalTaskId(task.getId()));
      }
    });
  }

  private void setSinkTasks() {
    taskGroupDag.topologicalDo(task -> {
      if (getOutEdgesToOtherStages(task).size() > 0) {
        sinkTasks.add(task);
        LOG.info("Sink Task: {} {}", taskGroupId, getPhysicalTaskId(task.getId()));
      }
    });
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
        final PipeImpl parentOutputPipe = physicalTaskIdToOutputPipeMap.get(getPhysicalTaskId(parent.getId()));
        inputPipes.add(parentOutputPipe);
        LOG.info("log: Added Outputpipe of {} as InputPipe of {} {}",
            getPhysicalTaskId(parent.getId()), taskGroupId, physicalTaskId);
      });
      physicalTaskIdToInputPipesMap.put(physicalTaskId, inputPipes);
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

    physicalTaskIdToOutputPipeMap.put(physicalTaskId, outputPipe);
    LOG.info("log: {} {} Added OutputPipe", taskGroupId, physicalTaskId);
  }

  private boolean hasInputPipe(final Task task) {
    return physicalTaskIdToInputPipesMap.containsKey(getPhysicalTaskId(task.getId()));
  }

  private void setTaskPutOnHold(final MetricCollectionBarrierTask task) {
    final String physicalTaskId = getPhysicalTaskId(task.getId());
    logicalTaskIdPutOnHold = RuntimeIdGenerator.getLogicalTaskIdIdFromPhysicalTaskId(physicalTaskId);
  }

  // Called only once by the sink task of this TaskGroup.
  // Empties the pipe of this sink task and form the inter-TaskGroup data.
  private void writeAndCloseOutputWriters() {
    sinkTasks.forEach(sinkTask -> {
      final String physicalTaskId = getPhysicalTaskId(sinkTask.getId());
      final PipeImpl pipe = physicalTaskIdToOutputPipeMap.get(physicalTaskId);
      final List output = pipe.collectOutputList();
      LOG.info("log: {} {}: sink task, Write to OutputWriter {}", taskGroupId, physicalTaskId, output);
      if (!output.isEmpty()) {
        outputWriters.forEach(outputWriter -> {
          outputWriter.write(output);
        });
      }
    });

    outputWriters.forEach(outputWriter -> outputWriter.close());
  }

  private void prepareInputFromSource() {
    sourceTasks.forEach(srcTask -> {
      if (srcTask instanceof BoundedSourceTask) {
        try {
          final Readable readable = ((BoundedSourceTask) srcTask).getReadable();
          final Iterable readData = readable.read();
          numPartitions++;
          final String iteratorId = generateIteratorId();
          idToIteratorMap.putIfAbsent(iteratorId, readData.iterator());
          iteratorIdToTasksMap.putIfAbsent(iteratorId, new ArrayList<>());
          iteratorIdToTasksMap.get(iteratorId).add(srcTask);
          LOG.info("iteratorId : Tasks {}", iteratorIdToTasksMap);
        } catch (final BlockFetchException ex) {
          taskGroupStateManager.onTaskGroupStateChanged(TaskGroupState.State.FAILED_RECOVERABLE,
              Optional.empty(), Optional.of(TaskGroupState.RecoverableFailureCause.INPUT_READ_FAILURE));
          LOG.info("{} Execution Failed (Recoverable: input read failure)! Exception: {}",
              taskGroupId, ex.toString());
        } catch (final Exception e) {
          taskGroupStateManager.onTaskGroupStateChanged(TaskGroupState.State.FAILED_UNRECOVERABLE,
              Optional.empty(), Optional.empty());
          LOG.info("{} Execution Failed! Exception: {}",
              taskGroupId, e.toString());
          throw new RuntimeException(e);
        }
      }
      // TODO #XXX: Support other types of source tasks, i. e. InitializedSourceTask
    });
  }

  private void prepareInputFromOtherStages() {
    inputReaders.stream()
        .filter(inputReader -> !inputReader.isSideInputReader())
        .forEach(inputReader -> {
          final List<CompletableFuture<Iterator>> futures = inputReader.read();
          numPartitions += futures.size();  // Aggregate total number of partitions to process
          // Add consumers which will push the data to the data queue when it ready to the futures.
          futures.forEach(compFuture -> compFuture.whenComplete((iterator, exception) -> {
            if (exception != null) {
              throw new BlockFetchException(exception);
            }

            final String iteratorId = generateIteratorId();
            idToIteratorMap.putIfAbsent(iteratorId, iterator);
            iteratorIdToTasksMap.putIfAbsent(iteratorId, new ArrayList<>());
            iteratorIdToTasksMap.get(iteratorId).addAll(inputReaderToTasksMap.get(inputReader));
            LOG.info("iteratorId : Tasks {}", iteratorIdToTasksMap);
          }));
        });
  }

  private boolean finishedSrcData() {
    boolean forAllPartitions = (idToIteratorMap.keySet().size() == numPartitions);
    boolean finished = true;
    for (Map.Entry<String, Iterator> entry : idToIteratorMap.entrySet()) {
      Iterator iterator = entry.getValue();
      if (iterator.hasNext()) {
        finished = false;
      }
    }
    return forAllPartitions && finished;
  }

  private boolean finishedInternalData() {
    boolean finished = true;

    /*
    for (Map.Entry<String, PipeImpl> entry : physicalTaskIdToOutputPipeMap.entrySet()) {
      final String physicalTaskId = entry.getKey();
      final PipeImpl pipe = entry.getValue();
      if (!pipe.isEmpty()) {
        LOG.info("{} Outputpipe of {} isn't empty!", taskGroupId, physicalTaskId);
        finished = false;
      }
    }*/

    for (PipeImpl pipe : pipeToDstTasksMap.keySet()) {
      if (!pipe.isEmpty()) {
        finished = false;
      }
    }

    return finished;
  }

  private void initializePipeToDstTasksMap() {
    iteratorIdToTasksMap.values().forEach(tasks -> {
      tasks.stream().forEach(task -> {
        final String physicalTaskId = getPhysicalTaskId(task.getId());
        final PipeImpl pipe = physicalTaskIdToOutputPipeMap.get(physicalTaskId);
        final List<Task> dstTasks = taskGroupDag.getChildren(task.getId());
        if (!dstTasks.isEmpty()) {
          pipeToDstTasksMap.putIfAbsent(pipe, dstTasks);
          LOG.info("{} pipeToDstTasksMap: [{}'s OutputPipe, {}]", taskGroupId, physicalTaskId, dstTasks);
        }
      });
    });
  }

  private void updatePipeToDstTasksMap() {
    // iteratorId : List[Tasks]
    // for each Tasks in each entry, build a map of
    // those Task's output pipe : List[those Task's children]
    Map<PipeImpl, List<Task>> currentMap = pipeToDstTasksMap;
    Map<PipeImpl, List<Task>> updatedMap = new HashMap<>();

    currentMap.values().forEach(tasks -> {
      tasks.stream().forEach(task -> {
        final String physicalTaskId = getPhysicalTaskId(task.getId());
        final PipeImpl pipe = physicalTaskIdToOutputPipeMap.get(physicalTaskId);
        final List<Task> dstTasks = taskGroupDag.getChildren(task.getId());
        if (!dstTasks.isEmpty()) {
          updatedMap.putIfAbsent(pipe, dstTasks);
          LOG.info("{} pipeToDstTasksMap: [{}'s OutputPipe, {}]", taskGroupId, physicalTaskId, dstTasks);
        }
      });
    });

    pipeToDstTasksMap = updatedMap;
  }

  private void closeTransforms(final List<Task> tasks) {
    tasks.stream().forEach(task -> {
      if (task instanceof OperatorTask) {
        Transform transform = ((OperatorTask) task).getTransform();
        transform.close();
        LOG.info("{} {} Closed transform {}", taskGroupId, getPhysicalTaskId(task.getId()), transform);
      }
    });
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
      // Source data denotes data from SourceTasks or other Stages.
      while (!finishedSrcData()) {
        iteratorIdToTasksMap.forEach((iteratorId, tasks) -> {
              Iterator iterator = idToIteratorMap.get(iteratorId);
              iterator.forEachRemaining(element -> {
                for (final Task task : tasks) {
                  List data = Collections.singletonList(element);
                  LOG.info("{} {} input to runTask {}", taskGroupId, getPhysicalTaskId(task.getId()), data);
                  runTask(task, data);
                }
              });
            }
        );

        // Now we're finished with processing source data.
        // Close OperatorTask transforms if needed.
        iteratorIdToTasksMap.values().forEach(this::closeTransforms);
      }
      LOG.info("{} Finished processing src data!", taskGroupId);

      // Process internal data.
      // Internal data denotes data from pipes of Tasks.
      initializePipeToDstTasksMap();
      while (!finishedInternalData()) {
        pipeToDstTasksMap.forEach((pipe, dstTasks) -> {
          // for all data in this pipe
          while (!pipe.isEmpty()) {
            // form output
            final List output;
            if (!pipe.isEmpty()) {
              output = Collections.singletonList(pipe.remove()); // intra-TaskGroup data are safe here
            } else {
              output = new ArrayList();
            }
            // and pass it to the dstTasks
            for (final Task task : dstTasks) {
              LOG.info("{} {} input to runTask {}", taskGroupId, getPhysicalTaskId(task.getId()), output);
              runTask(task, output);
            }
          }
        });
        // Now we're finished with this round of processing internal data.
        // Close OperatorTask transforms if needed.
        pipeToDstTasksMap.values().forEach(this::closeTransforms);

        updatePipeToDstTasksMap();
      }

      // Sink tasks write inter-Stage data to OutputWriters and close them.
      writeAndCloseOutputWriters();
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

  private Map<Transform, Object> sideInputFromOtherStages(final Task task, final Map<Transform, Object> sideInputMap) {
    srcTaskToSideInputReadersMap.get(task).forEach(sideInputReader -> {
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

    physicalTaskIdToInputPipesMap.get(physicalTaskId).stream().forEach(inputPipe -> {
      if (inputPipe.hasSideInputFor(physicalTaskId)) {
        Object sideInput = inputPipe.remove();   // because sideInput is only 1 element in the pipe
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

  /**
   * Processes an OperatorTask.
   *
   * @param task to execute
   */
  private void runTask(final Task task, final List<Object> data) {
    final String physicalTaskId = getPhysicalTaskId(task.getId());

    if (task instanceof BoundedSourceTask) {
      PipeImpl pipe = physicalTaskIdToOutputPipeMap.get(physicalTaskId);

      if (data.contains(null)) {  // this is [null] used for VoidCoders
        pipe.emit(data);
      } else if (data.size() > 0) {
        data.forEach(dataElement -> {
          pipe.emit(dataElement);
          LOG.info("log: {} {} BoundedSourceTask emitting {} to pipe",
              taskGroupId, physicalTaskId, dataElement);
        });
      }
    } else if (task instanceof OperatorTask) {
      final Transform transform = ((OperatorTask) task).getTransform();

      if (!preparedTransforms.contains(transform)) {
        final Map<Transform, Object> sideInputMap = new HashMap<>();

        // Check and collect side inputs.
        if (srcTaskToSideInputReadersMap.keySet().contains(task)) {
          sideInputFromOtherStages(task, sideInputMap);
        }
        if (hasInputPipe(task)) {
          sideInputFromThisStage(task, sideInputMap);
        }

        final Transform.Context transformContext = new ContextImpl(sideInputMap);
        final PipeImpl outputPipe = physicalTaskIdToOutputPipeMap.get(physicalTaskId);
        transform.prepare(transformContext, outputPipe);

        preparedTransforms.add(transform);
      }

      // Consumes the received element from incoming edges.
      // Calculate the number of inter-TaskGroup data to process.
      LOG.info("{} {} received data {}", taskGroupId, physicalTaskId, data);
      int numElements = data.size();
      LOG.info("log: {} {}: numElements {}", taskGroupId, physicalTaskId, numElements);

      IntStream.range(0, numElements).forEach(dataNum -> {
        Object dataElement = data.get(dataNum);
        LOG.info("log: {} {} Input to onData : {}", taskGroupId, physicalTaskId, dataElement);
        transform.onData(dataElement);
      });
    } else if (task instanceof MetricCollectionBarrierTask) {
      PipeImpl pipe = physicalTaskIdToOutputPipeMap.get(physicalTaskId);

      if (data.contains(null)) {  // this is [null]
        pipe.emit(data);
      } else if (data.size() > 0) {
        data.forEach(dataElement -> {
          pipe.emit(dataElement);
          LOG.info("log: {} {} MetricCollectionTask emitting {} to pipe",
              taskGroupId, physicalTaskId, dataElement);
        });
      }
      setTaskPutOnHold((MetricCollectionBarrierTask) task);
    }

    // If this task has children to process, recursively process the input.
    if (!sinkTasks.contains(task)) {
      List<Task> dstTasks = taskGroupDag.getChildren(task.getId());
      PipeImpl pipe = physicalTaskIdToOutputPipeMap.get(physicalTaskId);
      final List output;
      if (!pipe.isEmpty()) {
        output = Collections.singletonList(pipe.remove()); // intra-TaskGroup data are safe here
      } else {
        output = new ArrayList();
      }

      dstTasks.forEach(dstTask -> runTask(dstTask, output));
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
    return ITERATOR_PREFIX + iteratorIdGenerator.getAndIncrement();
  }
}
