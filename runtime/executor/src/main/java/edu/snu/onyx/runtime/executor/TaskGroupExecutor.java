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
package edu.snu.onyx.runtime.executor;

import edu.snu.onyx.common.ContextImpl;
import edu.snu.onyx.common.exception.BlockFetchException;
import edu.snu.onyx.common.exception.BlockWriteException;
import edu.snu.onyx.common.ir.Reader;
import edu.snu.onyx.common.ir.vertex.OperatorVertex;
import edu.snu.onyx.common.ir.vertex.transform.Transform;
import edu.snu.onyx.runtime.common.plan.RuntimeEdge;
import edu.snu.onyx.runtime.common.plan.physical.*;
import edu.snu.onyx.runtime.common.state.TaskGroupState;
import edu.snu.onyx.runtime.executor.datatransfer.DataTransferFactory;
import edu.snu.onyx.runtime.executor.datatransfer.InputReader;
import edu.snu.onyx.runtime.executor.datatransfer.PipeImpl;
import edu.snu.onyx.runtime.executor.datatransfer.OutputWriter;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
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

  private final TaskGroup taskGroup;
  private final TaskGroupStateManager taskGroupStateManager;
  private final List<PhysicalStageEdge> stageIncomingEdges;
  private final List<PhysicalStageEdge> stageOutgoingEdges;
  private final DataTransferFactory channelFactory;

  /**
   * Map of task IDs in this task group to their readers/writers.
   */
  private final Map<String, List<InputReader>> taskIdToInputReaderMap;
  private final Map<String, List<OutputWriter>> taskIdToOutputWriterMap;

  // For inter-TaskGroup data transfer, each task fetches intra-TaskGroup input
  // from its parents' Pipes. Here, we call the Pipes as following:
  // InputPipe: Parent tasks' Pipes and whether the data in it are side inputs
  // OutputPipe: This task's Pipes
  private final Map<String, List<PipeImpl>> taskIdToInputPipeMap;
  private final Map<String, PipeImpl> taskIdToOutputPipeMap;
  private final List<String> taskList;
  private boolean isExecutionRequested;

  public TaskGroupExecutor(final TaskGroup taskGroup,
                           final TaskGroupStateManager taskGroupStateManager,
                           final List<PhysicalStageEdge> stageIncomingEdges,
                           final List<PhysicalStageEdge> stageOutgoingEdges,
                           final DataTransferFactory channelFactory) {
    this.taskGroup = taskGroup;
    this.taskGroupStateManager = taskGroupStateManager;
    this.stageIncomingEdges = stageIncomingEdges;
    this.stageOutgoingEdges = stageOutgoingEdges;
    this.channelFactory = channelFactory;

    this.taskIdToInputReaderMap = new HashMap<>();
    this.taskIdToOutputWriterMap = new HashMap<>();
    this.taskIdToInputPipeMap = new HashMap<>();
    this.taskIdToOutputPipeMap = new HashMap<>();
    this.taskList = new ArrayList<>();

    this.isExecutionRequested = false;

    initializeDataTransfer();
  }

  /**
   * Initializes readers and writers depending on the execution properties.
   * Note that there are edges that are cross-stage and stage-internal.
   */
  private void initializeDataTransfer() {
    taskGroup.getTaskDAG().topologicalDo((task -> {
      final Set<PhysicalStageEdge> inEdgesFromOtherStages = getInEdgesFromOtherStages(task);
      final Set<PhysicalStageEdge> outEdgesToOtherStages = getOutEdgesToOtherStages(task);

      // Add InputReaders for inter-stage data transfer
      inEdgesFromOtherStages.forEach(physicalStageEdge -> {
        final InputReader inputReader = channelFactory.createReader(
            task, physicalStageEdge.getSrcVertex(), physicalStageEdge);
        addInputReader(task, inputReader);
      });

      // Add OutputWriters for inter-stage data transfer
      outEdgesToOtherStages.forEach(physicalStageEdge -> {
        final OutputWriter outputWriter = channelFactory.createWriter(
            task, physicalStageEdge.getDstVertex(), physicalStageEdge);
        addOutputWriter(task, outputWriter);
      });

      // Add InputPipes for intra-stage data transfer
      addInputPipe(task);

      // Add OutputPipe for intra-stage data transfer'
      addOutputPipe(task);

      taskList.add(task.getId());
    }));
    LOG.info("log: taskList: {}", taskList.size());
  }

  // Helper functions to initializes cross-stage edges.
  private Set<PhysicalStageEdge> getInEdgesFromOtherStages(final Task task) {
    return stageIncomingEdges.stream().filter(
        stageInEdge -> stageInEdge.getDstVertex().getId().equals(task.getRuntimeVertexId()))
        .collect(Collectors.toSet());
  }

  private Set<PhysicalStageEdge> getOutEdgesToOtherStages(final Task task) {
    return stageOutgoingEdges.stream().filter(
        stageInEdge -> stageInEdge.getSrcVertex().getId().equals(task.getRuntimeVertexId()))
        .collect(Collectors.toSet());
  }

  // Helper functions to add the initialized reader/writer to the maintained map.
  private void addInputReader(final Task task, final InputReader inputReader) {
    taskIdToInputReaderMap.computeIfAbsent(task.getId(), readerList -> new ArrayList<>());
    taskIdToInputReaderMap.get(task.getId()).add(inputReader);
  }

  private void addOutputWriter(final Task task, final OutputWriter outputWriter) {
    taskIdToOutputWriterMap.computeIfAbsent(task.getId(), readerList -> new ArrayList<>());
    taskIdToOutputWriterMap.get(task.getId()).add(outputWriter);
  }

  private void closeOutputWriters() {
    for (final Map.Entry<String, List<OutputWriter>> entry : taskIdToOutputWriterMap.entrySet()) {
      final String taskId = entry.getKey();
      final List<OutputWriter> outputWriters = entry.getValue();
      outputWriters.forEach(outputWriter -> {
        outputWriter.close();
        LOG.info("log: {} {} Closed OutputWriter(commited block!) {}",
            taskGroup.getTaskGroupId(), taskId, outputWriter.getId());
      });
    }
  }

  private void addInputPipe(final Task task) {
    List<PipeImpl> localPipes = new ArrayList<>();
    List<Task> parentTasks = taskGroup.getTaskDAG().getParents(task.getId());

    if (parentTasks != null) {
      parentTasks.forEach(parent -> {
        LOG.info("log: Adding InputPipe(Parents of {} {}: {})",
            taskGroup.getTaskGroupId(), task.getRuntimeVertexId(), parent.getRuntimeVertexId());
        localPipes.add(taskIdToOutputPipeMap.get(parent.getId()));
      });
      taskIdToInputPipeMap.put(task.getId(), localPipes);
    }
  }

  private void addOutputPipe(final Task task) {
    final PipeImpl outputPipe = new PipeImpl();

    final List<RuntimeEdge<Task>> outEdges = taskGroup.getTaskDAG().getOutgoingEdgesOf(task);
    outEdges.forEach(outEdge -> {
      outputPipe.setRuntimeEdge(outEdge);
      if (outEdge.isSideInput()) {
        outputPipe.markAsSideInput();
        LOG.info("log: {} {} Adding OutputPipe which will be a sideInput, edge {}",
            taskGroup.getTaskGroupId(), task.getRuntimeVertexId(), outEdge);
      }
    });

    taskIdToOutputPipeMap.put(task.getId(), outputPipe);
  }

  private boolean hasInputReader(final Task task) {
    return taskIdToInputReaderMap.containsKey(task.getId());
  }

  private boolean hasOutputWriter(final Task task) {
    return taskIdToOutputWriterMap.containsKey(task.getId());
  }

  private boolean hasInputPipe(final Task task) {
    return taskIdToInputPipeMap.containsKey(task.getId());
  }

  /**
   * Check whether this task has no intra-TaskGroup input left to process.
   *
   * @param task Task to check intra-TaskGroup input.
   * @return true if there is no intra-TaskGroup input left to be processed.
   */
  private boolean allInputPipesEmpty(final Task task) {
    if (taskIdToInputPipeMap.containsKey(task.getId())) {
      int nonEmptyInputPipes = 0;
      for (PipeImpl localReader : taskIdToInputPipeMap.get(task.getId())) {
        if (!localReader.isEmpty()) {
          nonEmptyInputPipes++;
        }
      }
      return nonEmptyInputPipes == 0;
    } else {
      return true;
    }
  }

  /**
   * Executes the task group.
   */
  public void execute() {
    if (isExecutionRequested) {
      throw new RuntimeException("TaskGroup {" + taskGroup.getTaskGroupId() + "} execution called again!");
    } else {
      isExecutionRequested = true;
    }

    taskGroupStateManager.onTaskGroupStateChanged(TaskGroupState.State.EXECUTING, Optional.empty(), Optional.empty());

    while (!isTaskGroupComplete()) {
      taskGroup.getTaskDAG().topologicalDo(task -> {
        try {
          if (taskList.contains(task.getId())) {
            if (task instanceof BoundedSourceTask) {
              launchBoundedSourceTask((BoundedSourceTask) task);
            } else if (task instanceof OperatorTask) {
              launchOperatorTask((OperatorTask) task);
              checkTaskCompletion((OperatorTask) task);
            } else if (task instanceof MetricCollectionBarrierTask) {
              launchMetricCollectionBarrierTask((MetricCollectionBarrierTask) task);
            } else {
              throw new UnsupportedOperationException(task.toString());
            }
          }
        } catch (final BlockFetchException ex) {
          taskGroupStateManager.onTaskGroupStateChanged(TaskGroupState.State.FAILED_RECOVERABLE,
              Optional.empty(), Optional.of(TaskGroupState.RecoverableFailureCause.INPUT_READ_FAILURE));
          LOG.info("{} Execution Failed (Recoverable: input read failure)! Exception: {}",
              taskGroup.getTaskGroupId(), ex.toString());
        } catch (final BlockWriteException ex2) {
          taskGroupStateManager.onTaskGroupStateChanged(TaskGroupState.State.FAILED_RECOVERABLE,
              Optional.empty(), Optional.of(TaskGroupState.RecoverableFailureCause.OUTPUT_WRITE_FAILURE));
          LOG.info("{} Execution Failed (Recoverable: output write failure)! Exception: {}",
              taskGroup.getTaskGroupId(), ex2.toString());
        } catch (final Exception e) {
          taskGroupStateManager.onTaskGroupStateChanged(TaskGroupState.State.FAILED_UNRECOVERABLE,
              Optional.empty(), Optional.empty());
          LOG.info("{} Execution Failed! Exception: {}",
              taskGroup.getTaskGroupId(), e.toString());
          throw new RuntimeException(e);
        }
      });
    }

    closeOutputWriters();

    taskGroupStateManager.onTaskGroupStateChanged(TaskGroupState.State.COMPLETE, Optional.empty(), Optional.empty());
    LOG.info("{} Complete!", taskGroup.getTaskGroupId());
  }

  private boolean allParentTasksComplete(final Task task) {
    // If there is a parent task that hasn't yet completed,
    // then this task isn't complete.
    List<Task> parentTasks = taskGroup.getTaskDAG().getParents(task.getId());
    AtomicInteger parentTasksNotYetComplete = new AtomicInteger(0);
    parentTasks.forEach(parentTask -> {
      if (taskList.contains(parentTask.getId())) {
        parentTasksNotYetComplete.getAndIncrement();
      }
    });

    return parentTasksNotYetComplete.get() == 0;
  }

  // A Task can be marked as 'Complete' when the following two conditions are both met:
  // - All of its LocalPipes are empty(if has any)
  // - All of its parent Tasks are complete
  private void checkTaskCompletion(final OperatorTask task) {
    if (allInputPipesEmpty(task) && allParentTasksComplete(task)) {
      if (taskList.contains(task.getId())) {
        taskList.remove(task.getId());
        LOG.info("log: {} {} Complete!", taskGroup.getTaskGroupId(), task.getId());
        LOG.info("log: pendingTasks: {}", taskList);
      }
    }
  }

  private boolean isTaskGroupComplete() {
    return taskList.isEmpty();
  }

  /**
   * Processes a BoundedSourceTask.
   *
   * @param sourceTask to execute
   * @throws Exception occurred during input read.
   */
  private void launchBoundedSourceTask(final BoundedSourceTask sourceTask) throws Exception {
    final Reader reader = sourceTask.getReader();
    final Iterator readData = reader.read();
    final List iterable = new ArrayList<>();
    readData.forEachRemaining(iterable::add);

    // Writes inter-TaskGroup data
    if (hasOutputWriter(sourceTask)) {
      taskIdToOutputWriterMap.get(sourceTask.getId()).forEach(outputWriter -> {
        outputWriter.write(iterable);
        //outputWriter.close();
      });
    } else {
      // Writes intra-TaskGroup data
      PipeImpl outputPipe = taskIdToOutputPipeMap.get(sourceTask.getId());
      try {
        iterable.forEach(data -> {
          outputPipe.emit(data);
          LOG.info("log: {} {} {} To OutputPipe wrote {}", taskGroup.getTaskGroupId(),
              sourceTask.getId(), sourceTask.getRuntimeVertexId(), data);
        });
      } catch (final Exception e) {
        throw new RuntimeException();
      }
    }

    taskList.remove(sourceTask.getId());
    LOG.info("log: {} {} Complete!", taskGroup.getTaskGroupId(), sourceTask.getId());
  }

  /**
   * Processes an OperatorTask.
   *
   * @param operatorTask to execute
   */
  private void launchOperatorTask(final OperatorTask operatorTask) {
    final Map<Transform, Object> sideInputMap = new HashMap<>();

    // Check for side inputs
    if (hasInputReader(operatorTask)) {
      taskIdToInputReaderMap.get(operatorTask.getId()).stream().filter(InputReader::isSideInputReader)
          .forEach(sideInputReader -> {
            try {
              final Object sideInput = sideInputReader.getSideInput().get();
              final RuntimeEdge inEdge = sideInputReader.getRuntimeEdge();
              final Transform srcTransform;
              if (inEdge instanceof PhysicalStageEdge) {
                srcTransform = ((OperatorVertex) ((PhysicalStageEdge) inEdge).getSrcVertex())
                    .getTransform();
              } else {
                srcTransform = ((OperatorTask) inEdge.getSrc()).getTransform();
              }
              sideInputMap.put(srcTransform, sideInput);
              LOG.info("log: {} {} sideInput from InputReader {}", taskGroup.getTaskGroupId(), operatorTask.getId(),
                  sideInput);
            } catch (final InterruptedException | ExecutionException e) {
              throw new BlockFetchException(e);
            }
          });
    }

    if (hasInputPipe(operatorTask)) {
      taskIdToInputPipeMap.get(operatorTask.getId()).stream().filter(PipeImpl::isSideInput)
          .forEach(sideInputPipe -> {
            final Object sideInput = sideInputPipe.remove();
            final RuntimeEdge inEdge = sideInputPipe.getRuntimeEdge();
            final Transform srcTransform;
            if (inEdge instanceof PhysicalStageEdge) {
              srcTransform = ((OperatorVertex) ((PhysicalStageEdge) inEdge).getSrcVertex())
                  .getTransform();
            } else {
              srcTransform = ((OperatorTask) inEdge.getSrc()).getTransform();
            }
            sideInputMap.put(srcTransform, sideInput);
            LOG.info("log: {} {} sideInput from InputPipe {}", taskGroup.getTaskGroupId(), operatorTask.getId(),
                sideInput);
          });
    }

    final Transform.Context transformContext = new ContextImpl(sideInputMap);
    final Transform transform = operatorTask.getTransform();
    final PipeImpl outputPipe = taskIdToOutputPipeMap.get(operatorTask.getId());
    transform.prepare(transformContext, outputPipe);

    // Check for non-side inputs.
    final ArrayDeque<Object> dataQueue = new ArrayDeque<>();
    if (hasInputReader(operatorTask)) {
      taskIdToInputReaderMap.get(operatorTask.getId()).stream()
          .filter(inputReader -> !inputReader.isSideInputReader()).forEach(inputReader -> {
        List<CompletableFuture<Iterator>> futures = inputReader.read();
        futures.forEach(compFuture -> {
          try {
            Iterator iterator = compFuture.get();
            iterator.forEachRemaining(data -> {
              if (data != null) {
                dataQueue.add(data);
                LOG.info("log: {} {} Read from InputReader : {}",
                    taskGroup.getTaskGroupId(), operatorTask.getId(), data);
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

    if (hasInputPipe(operatorTask)) {
      // Reads intra-TaskGroup data.
      for (PipeImpl localReader : taskIdToInputPipeMap.get(operatorTask.getId())) {
        if (!localReader.isEmpty() && !localReader.isSideInput()) {
          Object data = localReader.remove();
          if (data != null) {
            dataQueue.add(data);
            LOG.info("log: {} {} Read from InputPipe : {}",
                taskGroup.getTaskGroupId(), operatorTask.getId(), data);
          }
        }
      }
    }

    // Consumes the received element from incoming edges.
    // Calculate the number of inter-TaskGroup data to process.
    int numElements = dataQueue.size();
    LOG.info("log: {} {}: numElements {}", taskGroup.getTaskGroupId(), operatorTask.getId(), numElements);

    IntStream.range(0, numElements).forEach(dataNum -> {
      Object data = dataQueue.pop();
      LOG.info("log: {} {} Input to onData : {}",
          taskGroup.getTaskGroupId(), operatorTask.getId(), data);

      if (data != null) {
        transform.onData(data);
      }

      // If there is any output, write to OutputWriter.
      //if (hasOutputWriter(operatorTask)) {
      //  writeToOutputWriter(outputPipe, operatorTask);
      //}
    });

    transform.close();
    // If there is any output, write to OutputWriter.
    if (hasOutputWriter(operatorTask)) {
      writeToOutputWriter(outputPipe, operatorTask);
    }
  }

  private void writeToOutputWriter(final PipeImpl localWriter, final Task operatorTask) {
    final List output = localWriter.collectOutputList();
    if (!output.isEmpty()) {
      taskIdToOutputWriterMap.get(operatorTask.getId()).forEach(outputWriter -> {
        outputWriter.write(output);
        LOG.info("log: {} {}: Write to OutputWriter {}", taskGroup.getTaskGroupId(),
            operatorTask.getId(), output);
        //outputWriter.close();
      });
    }
  }

  /**
   * Pass on the data to the following tasks.
   *
   * @param task the task to carry on the data.
   */
  private void launchMetricCollectionBarrierTask(final MetricCollectionBarrierTask task) {
    final BlockingQueue<Iterator> dataQueue = new LinkedBlockingQueue<>();
    final AtomicInteger sourceParallelism = new AtomicInteger(0);
    taskIdToInputReaderMap.get(task.getId()).stream().filter(inputReader -> !inputReader.isSideInputReader())
        .forEach(inputReader -> {
          sourceParallelism.getAndAdd(inputReader.getSourceParallelism());
          inputReader.read().forEach(compFuture -> compFuture.thenAccept(dataQueue::add));
        });

    final List data = new ArrayList<>();
    IntStream.range(0, sourceParallelism.get()).forEach(srcTaskNum -> {
      try {
        final Iterator availableData = dataQueue.take();
        availableData.forEachRemaining(data::add);
      } catch (final InterruptedException e) {
        throw new BlockFetchException(e);
      }
    });
    taskIdToOutputWriterMap.get(task.getId()).forEach(outputWriter -> {
      outputWriter.write(data);
    });
  }
}
