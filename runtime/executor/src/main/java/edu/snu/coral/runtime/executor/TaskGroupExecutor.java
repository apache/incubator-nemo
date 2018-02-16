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
import edu.snu.coral.common.ir.Readable;
import edu.snu.coral.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.coral.common.ir.executionproperty.ExecutionProperty;
import edu.snu.coral.common.ir.vertex.transform.Transform;
import edu.snu.coral.common.ir.vertex.OperatorVertex;
import edu.snu.coral.runtime.common.RuntimeIdGenerator;
import edu.snu.coral.runtime.common.plan.RuntimeEdge;
import edu.snu.coral.runtime.common.plan.physical.*;
import edu.snu.coral.runtime.common.state.TaskGroupState;
import edu.snu.coral.runtime.executor.datatransfer.DataTransferFactory;
import edu.snu.coral.runtime.executor.datatransfer.InputReader;
import edu.snu.coral.runtime.executor.datatransfer.OutputWriter;
import edu.snu.coral.runtime.executor.datatransfer.PipeImpl;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

  /**
   * Map of task IDs in this task group to their readers/writers.
   */
  private final Map<String, List<InputReader>> physicalTaskIdToInputReaderMap;
  private final Map<String, List<OutputWriter>> physicalTaskIdToOutputWriterMap;

  // For inter-TaskGroup data transfer, each task fetches intra-TaskGroup input
  // from its parents' Pipes. Here, we call the Pipes as following:
  // InputPipe: Parent tasks' Pipes and whether the data in it are side inputs
  // OutputPipe: This task's Pipes
  private final Map<String, List<PipeImpl>> physicalTaskIdToInputPipeMap;
  private final Map<String, PipeImpl> physicalTaskIdToOutputPipeMap;
  private final List<String> taskList;
  private boolean isExecutionRequested;
  private boolean isTaskGroupOnHold;

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

    this.physicalTaskIdToInputReaderMap = new HashMap<>();
    this.physicalTaskIdToOutputWriterMap = new HashMap<>();
    this.physicalTaskIdToInputPipeMap = new HashMap<>();
    this.physicalTaskIdToOutputPipeMap = new HashMap<>();
    this.taskList = new ArrayList<>();

    this.isExecutionRequested = false;
    this.isTaskGroupOnHold = false;

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
    taskGroupDag.topologicalDo((task -> {
      final Set<PhysicalStageEdge> inEdgesFromOtherStages = getInEdgesFromOtherStages(task);
      final Set<PhysicalStageEdge> outEdgesToOtherStages = getOutEdgesToOtherStages(task);
      final String physicalTaskId = getPhysicalTaskId(task.getId());

      // Add InputReaders for inter-stage data transfer
      inEdgesFromOtherStages.forEach(physicalStageEdge -> {
        final InputReader inputReader = channelFactory.createReader(
            taskGroupIdx, physicalStageEdge.getSrcVertex(), physicalStageEdge);
        addInputReader(task, inputReader);
      });

      // Add OutputWriters for inter-stage data transfer
      outEdgesToOtherStages.forEach(physicalStageEdge -> {
        final OutputWriter outputWriter = channelFactory.createWriter(
            task, taskGroupIdx, physicalStageEdge.getDstVertex(), physicalStageEdge);
        addOutputWriter(task, outputWriter);
        LOG.info("log: Adding OutputWriter {} {} {}", taskGroupId, task.getIrVertexId(), outputWriter);
      });

      // Add InputPipes for intra-stage data transfer
      addInputPipe(task);

      // Add OutputPipe for intra-stage data transfer
      addOutputPipe(task);

      taskList.add(physicalTaskId);
    }));
    LOG.info("log: taskList: {}", taskList.size());
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

  // Helper functions to add the initialized reader/writer to the maintained map.
  private void addInputReader(final Task task, final InputReader inputReader) {
    final String physicalTaskId = getPhysicalTaskId(task.getId());
    physicalTaskIdToInputReaderMap.computeIfAbsent(physicalTaskId, readerList -> new ArrayList<>());
    physicalTaskIdToInputReaderMap.get(physicalTaskId).add(inputReader);
  }

  private void addOutputWriter(final Task task, final OutputWriter outputWriter) {
    final String physicalTaskId = getPhysicalTaskId(task.getId());
    physicalTaskIdToOutputWriterMap.computeIfAbsent(physicalTaskId, readerList -> new ArrayList<>());
    physicalTaskIdToOutputWriterMap.get(physicalTaskId).add(outputWriter);
  }

  private void addInputPipe(final Task task) {
    List<PipeImpl> localPipes = new ArrayList<>();
    List<Task> parentTasks = taskGroupDag.getParents(task.getId());
    final String physicalTaskId = getPhysicalTaskId(task.getId());

    if (parentTasks != null) {
      parentTasks.forEach(parent -> {
        LOG.info("log: Adding InputPipe(Parents of {} {}: {})",
            taskGroupId, task.getIrVertexId(), parent.getIrVertexId());
        final String physicalParentTaskId = getPhysicalTaskId(parent.getId());
        localPipes.add(physicalTaskIdToOutputPipeMap.get(physicalParentTaskId));
      });
      physicalTaskIdToInputPipeMap.put(physicalTaskId, localPipes);
    }
  }

  private void addOutputPipe(final Task task) {
    final PipeImpl outputPipe = new PipeImpl();
    final String physicalTaskId = getPhysicalTaskId(task.getId());

    final List<RuntimeEdge<Task>> outEdges = taskGroupDag.getOutgoingEdgesOf(task);
    outEdges.forEach(outEdge -> {
      outputPipe.setRuntimeEdge(outEdge);
      if (outEdge.isSideInput()) {
        outputPipe.markAsSideInput();
        LOG.info("log: {} {} Adding OutputPipe which will be a sideInput, edge {}",
            taskGroupId, task.getIrVertexId(), outEdge);
      }
    });

    physicalTaskIdToOutputPipeMap.put(physicalTaskId, outputPipe);
  }

  private boolean hasInputReader(final Task task) {
    return physicalTaskIdToInputReaderMap.containsKey(getPhysicalTaskId(task.getId()));
  }

  private boolean hasOutputWriter(final Task task) {
    return physicalTaskIdToOutputWriterMap.containsKey(getPhysicalTaskId(task.getId()));
  }

  private boolean hasInputPipe(final Task task) {
    return physicalTaskIdToInputPipeMap.containsKey(getPhysicalTaskId(task.getId()));
  }

  private void writeToOutputWriter(final PipeImpl localWriter, final Task task) {
    final String physicalTaskId = getPhysicalTaskId(task.getId());
    final List output = localWriter.collectOutputList();
    if (!output.isEmpty()) {
      physicalTaskIdToOutputWriterMap.get(physicalTaskId).forEach(outputWriter -> {
        outputWriter.write(output);
        LOG.info("log: {} {}: Write to OutputWriter {}", taskGroupId, physicalTaskId, output);
      });
    }
  }

  private void closeOutputWriters() {
    for (final Map.Entry<String, List<OutputWriter>> entry : physicalTaskIdToOutputWriterMap.entrySet()) {
      final String physicalTaskId = entry.getKey();
      final List<OutputWriter> outputWriters = entry.getValue();
      outputWriters.forEach(outputWriter -> {
        outputWriter.close();
        LOG.info("log: {} {} Closed OutputWriter(commited block!) {}",
            taskGroupId, physicalTaskId, outputWriter.getId());
      });
    }
  }

  // TODO #737: Make AggregationTransform ReshapingPass to do this work at Complier side.
  private boolean aggregationNeeded(final Task task) {
    // If this task's outEdge is annotated as Broadcast,
    // this task should work on all input data and process an Iterable.
    for (RuntimeEdge outEdge : getOutEdgesToOtherStages(task)) {
      if (outEdge.getProperty(ExecutionProperty.Key.DataCommunicationPattern)
          .equals(DataCommunicationPatternProperty.Value.BroadCast)) {
        return true;
      }
    }
    return false;
  }


  /**
   * Check whether this task has no intra-TaskGroup input left to process.
   *
   * @param task Task to check intra-TaskGroup input.
   * @return true if there is no intra-TaskGroup input left to be processed.
   */
  private boolean allInputPipesEmpty(final Task task) {
    if (physicalTaskIdToInputPipeMap.containsKey(getPhysicalTaskId(task.getId()))) {
      int nonEmptyInputPipes = 0;
      for (PipeImpl localReader : physicalTaskIdToInputPipeMap.get(getPhysicalTaskId(task.getId()))) {
        if (!localReader.isEmpty()) {
          nonEmptyInputPipes++;
        }
      }
      return nonEmptyInputPipes == 0;
    } else {
      return true;
    }
  }

  private boolean allParentTasksComplete(final Task task) {
    // If there is a parent task that hasn't yet completed,
    // then this task isn't complete.
    List<Task> parentTasks = taskGroupDag.getParents(task.getId());
    AtomicInteger parentTasksNotYetComplete = new AtomicInteger(0);
    parentTasks.forEach(parentTask -> {
      if (taskList.contains(getPhysicalTaskId(parentTask.getId()))) {
        parentTasksNotYetComplete.getAndIncrement();
      }
    });

    return parentTasksNotYetComplete.get() == 0;
  }

  private void checkBoundedSourceTaskCompletion(final BoundedSourceTask task) {
    final String physicalTaskId = getPhysicalTaskId(task.getId());
    taskList.remove(getPhysicalTaskId(physicalTaskId));
    LOG.info("log: {} {} Complete!", taskGroupId, physicalTaskId);
  }

  private void checkOperatorTaskCompletion(final OperatorTask task) {
    final String physicalTaskId = getPhysicalTaskId(task.getId());
    if (allInputPipesEmpty(task) && allParentTasksComplete(task)) {
      task.getTransform().close();
      if (hasOutputWriter(task)) {
        writeToOutputWriter(physicalTaskIdToOutputPipeMap.get(physicalTaskId), task);
      }
      taskList.remove(physicalTaskId);
      LOG.info("log: {} {} Complete!", taskGroupId, task.getId());
      LOG.info("log: pendingTasks: {}", taskList);
    }
  }

  private void checkMetricCollectionBarrierTaskCompletion(final MetricCollectionBarrierTask task) {
    final String physicalTaskId = getPhysicalTaskId(task.getId());
    if (allInputPipesEmpty(task) && allParentTasksComplete(task)) {
      if (hasOutputWriter(task)) {
        writeToOutputWriter(physicalTaskIdToOutputPipeMap.get(physicalTaskId), task);
      }
      taskGroupStateManager.onTaskGroupStateChanged(TaskGroupState.State.ON_HOLD,
          Optional.of(physicalTaskId), Optional.empty());
      isTaskGroupOnHold = true;
      taskList.remove(physicalTaskId);
      LOG.info("log: {} {} Complete / ON_HOLD!", taskGroupId, physicalTaskId);
      LOG.info("log: pendingTasks: {}", taskList);
    }
  }

  // A Task can be marked as 'Complete' when the following two conditions are both met:
  // - All of its LocalPipes are empty(if has any)
  // - All of its parent Tasks are complete
  private void checkTaskCompletion(final Task task) {
    if (task instanceof BoundedSourceTask) {
      checkBoundedSourceTaskCompletion((BoundedSourceTask) task);
    } else if (task instanceof OperatorTask) {
      checkOperatorTaskCompletion((OperatorTask) task);
    } else if (task instanceof MetricCollectionBarrierTask) {
      checkMetricCollectionBarrierTaskCompletion((MetricCollectionBarrierTask) task);
    }
  }

  private boolean allTasksAreComplete() {
    return taskList.isEmpty();
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

    while (!allTasksAreComplete()) {
      taskGroupDag.topologicalDo(task -> {
        final String physicalTaskId = getPhysicalTaskId(task.getId());
        try {
          if (taskList.contains(physicalTaskId)) {
            if (task instanceof BoundedSourceTask) {
              launchBoundedSourceTask((BoundedSourceTask) task);
            } else if (task instanceof OperatorTask) {
              if (!aggregationNeeded(task)
                  || (aggregationNeeded(task) && allParentTasksComplete(task))) {
                launchOperatorTask((OperatorTask) task);
              }
            } else if (task instanceof MetricCollectionBarrierTask) {
              launchMetricCollectionBarrierTask((MetricCollectionBarrierTask) task);
            } else {
              throw new UnsupportedOperationException(task.toString());
            }

            checkTaskCompletion(task);
          }
        } catch (final BlockFetchException ex) {
          taskGroupStateManager.onTaskGroupStateChanged(TaskGroupState.State.FAILED_RECOVERABLE,
              Optional.empty(), Optional.of(TaskGroupState.RecoverableFailureCause.INPUT_READ_FAILURE));
          LOG.info("{} Execution Failed (Recoverable: input read failure)! Exception: {}",
              taskGroupId, ex.toString());
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
      });
    }

    closeOutputWriters();

    if (!isTaskGroupOnHold) {
      taskGroupStateManager.onTaskGroupStateChanged(TaskGroupState.State.COMPLETE, Optional.empty(), Optional.empty());
    }

    LOG.info("{} Complete!", taskGroupId);
  }

  /**
   * Processes a BoundedSourceTask.
   *
   * @param boundedSourceTask the bounded source task to execute
   * @throws Exception occurred during input read.
   */
  private void launchBoundedSourceTask(final BoundedSourceTask boundedSourceTask) throws Exception {

    final Readable readable = boundedSourceTask.getReadable();
    final Iterable readData = readable.read();

    final String physicalTaskId = getPhysicalTaskId(boundedSourceTask.getId());

    // Writes inter-TaskGroup data
    if (hasOutputWriter(boundedSourceTask)) {
      physicalTaskIdToOutputWriterMap.get(physicalTaskId).forEach(
          outputWriter -> outputWriter.write(readData));
    } else {
      // Writes intra-TaskGroup data
      PipeImpl outputPipe = physicalTaskIdToOutputPipeMap.get(physicalTaskId);
      try {
        if (((List) readData).contains(null)) {
          outputPipe.emit(readData);
        } else {
          readData.forEach(data -> outputPipe.emit(data));
        }
      } catch (final Exception e) {
        throw new RuntimeException();
      }
    }
  }

  /**
   * Processes an OperatorTask.
   *
   * @param task to execute
   */
  private void launchOperatorTask(final OperatorTask task) {
    final Map<Transform, Object> sideInputMap = new HashMap<>();
    final String physicalTaskId = getPhysicalTaskId(task.getId());

    // Check for side inputs
    if (hasInputReader(task)) {
      physicalTaskIdToInputReaderMap.get(physicalTaskId).stream().filter(InputReader::isSideInputReader)
          .forEach(sideInputReader -> {
            try {
              final Object sideInput = sideInputReader.getSideInput();
              final RuntimeEdge inEdge = sideInputReader.getRuntimeEdge();
              final Transform srcTransform;
              if (inEdge instanceof PhysicalStageEdge) {
                srcTransform = ((OperatorVertex) ((PhysicalStageEdge) inEdge).getSrcVertex())
                    .getTransform();
              } else {
                srcTransform = ((OperatorTask) inEdge.getSrc()).getTransform();
              }
              sideInputMap.put(srcTransform, sideInput);
              LOG.info("log: {} {} sideInput from InputReader {}", taskGroupId, task.getId(),
                  sideInput);
            } catch (final InterruptedException | ExecutionException e) {
              throw new BlockFetchException(e);
            }
          });
    }

    if (hasInputPipe(task)) {
      physicalTaskIdToInputPipeMap.get(physicalTaskId).stream().filter(PipeImpl::isSideInput)
          .forEach(sideInputPipe -> {
            Object sideInput = sideInputPipe.remove();
            final RuntimeEdge inEdge = sideInputPipe.getRuntimeEdge();
            final Transform srcTransform;
            if (inEdge instanceof PhysicalStageEdge) {
              srcTransform = ((OperatorVertex) ((PhysicalStageEdge) inEdge).getSrcVertex()).getTransform();
            } else {
              srcTransform = ((OperatorTask) inEdge.getSrc()).getTransform();
            }
            sideInputMap.put(srcTransform, sideInput);
            LOG.info("log: {} {} sideInput from InputPipe {}", taskGroupId, task.getId(), sideInput);
          });
    }

    final Transform.Context transformContext = new ContextImpl(sideInputMap);
    final Transform transform = task.getTransform();
    final PipeImpl outputPipe = physicalTaskIdToOutputPipeMap.get(physicalTaskId);
    transform.prepare(transformContext, outputPipe);

    // Check for non-side inputs.
    final ArrayDeque<Object> dataQueue = new ArrayDeque<>();
    if (hasInputReader(task)) {
      physicalTaskIdToInputReaderMap.get(physicalTaskId).stream()
          .filter(inputReader -> !inputReader.isSideInputReader())
          .forEach(inputReader -> {
            List<CompletableFuture<Iterator>> futures = inputReader.read();

            /*
            // Add consumers which will push the data to the data queue when it ready to the futures.
            futures.forEach(compFuture -> compFuture.whenComplete((data, exception) -> {
            if (exception != null) {
              throw new BlockFetchException(exception);
            }
            dataQueue.add(Pair.of(data, srcIrVtxId));
            }
            */
            futures.forEach(compFuture -> {
              try {
                Iterator iterator = compFuture.get();
                iterator.forEachRemaining(data -> {
                  if (data != null) {
                    dataQueue.add(data);
                    LOG.info("log: {} {} Read from InputReader : {}",
                        taskGroupId, task.getId(), data);
                  } else {
                    List<Object> iterable = Collections.singletonList(data);
                    dataQueue.add(iterable);
                  }
                });
              } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted while waiting for InputReader.readElement()", e);
              } catch (ExecutionException e1) {
                throw new RuntimeException("ExecutionException while waiting for InputReader.readElement()", e1);
              }
            });
          });
    }

    if (hasInputPipe(task)) {
      physicalTaskIdToInputPipeMap.get(physicalTaskId).stream()
          .filter(localReader -> !localReader.isEmpty() && !localReader.isSideInput())
          .forEach(localReader -> {
            if (aggregationNeeded(task)) {
              List<Object> iterable = localReader.collectOutputList();
              dataQueue.add(iterable);
              //LOG.info("log: {} {} Read from InputPipe : {}",
              //    taskGroupId, task.getId(), iterable);
            } else {
              Object data = localReader.remove();
              if (data != null) {
                dataQueue.add(data);
                LOG.info("log: {} {} Read from InputPipe : {}",
                    taskGroupId, task.getId(), data);
              } else {
                List<Object> iterable = Collections.singletonList(data);
                dataQueue.add(iterable);
              }
            }
          });
    }

    // Consumes the received element from incoming edges.
    // Calculate the number of inter-TaskGroup data to process.
    int numElements = dataQueue.size();
    LOG.info("log: {} {}: numElements {}", taskGroupId, task.getId(), numElements);

    IntStream.range(0, numElements).forEach(dataNum -> {
      Object data = dataQueue.pop();
      LOG.info("log: {} {} Input to onData : {}", taskGroupId, physicalTaskId, data);
      transform.onData(data);
    });
  }

  /**
   * Pass on the data to the following tasks.
   *
   * @param task the task to carry on the data.
   */
  private void launchMetricCollectionBarrierTask(final MetricCollectionBarrierTask task) {
    final String physicalTaskId = getPhysicalTaskId(task.getId());
    final PipeImpl<Object> pipe = physicalTaskIdToOutputPipeMap.get(physicalTaskId);

    // Check for non-side inputs.
    if (hasInputReader(task)) {
      physicalTaskIdToInputReaderMap.get(physicalTaskId).stream()
          .filter(inputReader -> !inputReader.isSideInputReader())
          .forEach(inputReader -> inputReader.read()
              .forEach(compFuture -> {
                compFuture.thenAccept(iterator ->
                    iterator.forEachRemaining(data -> {
                      if (data != null) {
                        pipe.emit(data);
                        LOG.info("log: {} {} Read from InputReader : {}",
                            taskGroupId, physicalTaskId, data);
                      } else {
                        List<Object> iterable = Collections.singletonList(data);
                        pipe.emit(iterable);
                      }
                    })
                );
              })
          );
    }

    if (hasInputPipe(task)) {
      physicalTaskIdToInputPipeMap.get(physicalTaskId).stream()
          .filter(localReader -> !localReader.isEmpty() && !localReader.isSideInput())
          .forEach(localReader -> {
            if (aggregationNeeded(task)) {
              List<Object> iterable = localReader.collectOutputList();
              pipe.emit(iterable);
            } else {
              Object data = localReader.remove();
              if (data != null) {
                pipe.emit(data);
                LOG.info("log: {} {} Read from InputPipe : {}",
                    taskGroupId, physicalTaskId, data);
              } else {
                List<Object> iterable = Collections.singletonList(data);
                pipe.emit(iterable);
              }
            }
          });
    }
  }

  /**
   * @param logicalTaskId the logical task id.
   * @return the physical task id.
   */
  private String getPhysicalTaskId(final String logicalTaskId) {
    return RuntimeIdGenerator.generatePhysicalTaskId(taskGroupIdx, logicalTaskId);
  }
}
