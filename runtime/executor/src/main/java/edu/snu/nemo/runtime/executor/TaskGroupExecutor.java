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
import edu.snu.nemo.runtime.common.state.TaskState;
import edu.snu.nemo.runtime.executor.datatransfer.DataTransferFactory;
import edu.snu.nemo.runtime.executor.datatransfer.InputReader;
import edu.snu.nemo.runtime.executor.datatransfer.OutputCollectorImpl;
import edu.snu.nemo.runtime.executor.datatransfer.OutputWriter;

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

  private final DAG<Task, RuntimeEdge<Task>> taskGroupDag;
  private final String taskGroupId;
  private final int taskGroupIdx;
  private final TaskGroupStateManager taskGroupStateManager;
  private final List<PhysicalStageEdge> stageIncomingEdges;
  private final List<PhysicalStageEdge> stageOutgoingEdges;
  private final DataTransferFactory channelFactory;
  private final MetricCollector metricCollector;

  /**
   * Map of task IDs in this task group to their readers/writers.
   */
  private final Map<String, List<InputReader>> physicalTaskIdToInputReaderMap;
  private final Map<String, List<OutputWriter>> physicalTaskIdToOutputWriterMap;

  private boolean isExecutionRequested;

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

    this.physicalTaskIdToInputReaderMap = new HashMap<>();
    this.physicalTaskIdToOutputWriterMap = new HashMap<>();

    this.isExecutionRequested = false;

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
    taskGroupDag.topologicalDo((task -> {
      final Set<PhysicalStageEdge> inEdgesFromOtherStages = getInEdgesFromOtherStages(task);
      final Set<PhysicalStageEdge> outEdgesToOtherStages = getOutEdgesToOtherStages(task);

      inEdgesFromOtherStages.forEach(physicalStageEdge -> {
        final InputReader inputReader = channelFactory.createReader(
            taskGroupIdx, physicalStageEdge.getSrcVertex(), physicalStageEdge);
        addInputReader(task, inputReader);
      });

      outEdgesToOtherStages.forEach(physicalStageEdge -> {
        final OutputWriter outputWriter = channelFactory.createWriter(
            task, taskGroupIdx, physicalStageEdge.getDstVertex(), physicalStageEdge);
        addOutputWriter(task, outputWriter);
      });

      final List<RuntimeEdge<Task>> inEdgesWithinStage = taskGroupDag.getIncomingEdgesOf(task);
      inEdgesWithinStage.forEach(internalEdge -> createLocalReader(task, internalEdge));

      final List<RuntimeEdge<Task>> outEdgesWithinStage = taskGroupDag.getOutgoingEdgesOf(task);
      outEdgesWithinStage.forEach(internalEdge -> createLocalWriter(task, internalEdge));
    }));
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

  // Helper functions to initializes stage-internal edges.
  private void createLocalReader(final Task task, final RuntimeEdge<Task> internalEdge) {
    final InputReader inputReader = channelFactory.createLocalReader(taskGroupIdx, internalEdge);
    addInputReader(task, inputReader);
  }

  private void createLocalWriter(final Task task, final RuntimeEdge<Task> internalEdge) {
    final OutputWriter outputWriter = channelFactory.createLocalWriter(task, taskGroupIdx, internalEdge);
    addOutputWriter(task, outputWriter);
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

  /**
   * Executes the task group.
   */
  public void execute() {
    LOG.info("{} Execution Started!", taskGroupId);
    if (isExecutionRequested) {
      throw new RuntimeException("TaskGroup {" + taskGroupId + "} execution called again!");
    } else {
      isExecutionRequested = true;
    }

    taskGroupStateManager.onTaskGroupStateChanged(
        TaskGroupState.State.EXECUTING, Optional.empty(), Optional.empty());

    taskGroupDag.topologicalDo(task -> {
      final String physicalTaskId = getPhysicalTaskId(task.getId());
      taskGroupStateManager.onTaskStateChanged(physicalTaskId, TaskState.State.EXECUTING, Optional.empty());
      try {
        if (task instanceof BoundedSourceTask) {
          launchBoundedSourceTask((BoundedSourceTask) task);
          taskGroupStateManager.onTaskStateChanged(physicalTaskId, TaskState.State.COMPLETE, Optional.empty());
          LOG.info("{} Execution Complete!", taskGroupId);
        } else if (task instanceof OperatorTask) {
          launchOperatorTask((OperatorTask) task);
          taskGroupStateManager.onTaskStateChanged(physicalTaskId, TaskState.State.COMPLETE, Optional.empty());
          LOG.info("{} Execution Complete!", taskGroupId);
        } else if (task instanceof MetricCollectionBarrierTask) {
          launchMetricCollectionBarrierTask((MetricCollectionBarrierTask) task);
          taskGroupStateManager.onTaskStateChanged(physicalTaskId, TaskState.State.ON_HOLD, Optional.empty());
          LOG.info("{} Execution Complete!", taskGroupId);
        } else {
          throw new UnsupportedOperationException(task.toString());
        }
      } catch (final BlockFetchException ex) {
        taskGroupStateManager.onTaskStateChanged(physicalTaskId, TaskState.State.FAILED_RECOVERABLE,
            Optional.of(TaskGroupState.RecoverableFailureCause.INPUT_READ_FAILURE));
        LOG.warn("{} Execution Failed (Recoverable)! Exception: {}",
            new Object[] {taskGroupId, ex.toString()});
      } catch (final BlockWriteException ex2) {
        taskGroupStateManager.onTaskStateChanged(physicalTaskId, TaskState.State.FAILED_RECOVERABLE,
            Optional.of(TaskGroupState.RecoverableFailureCause.OUTPUT_WRITE_FAILURE));
        LOG.warn("{} Execution Failed (Recoverable)! Exception: {}",
            new Object[] {taskGroupId, ex2.toString()});
      } catch (final Exception e) {
        taskGroupStateManager.onTaskStateChanged(
            physicalTaskId, TaskState.State.FAILED_UNRECOVERABLE, Optional.empty());
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Processes a BoundedSourceTask.
   *
   * @param boundedSourceTask the bounded source task to execute
   * @throws Exception occurred during input read.
   */
  private void launchBoundedSourceTask(final BoundedSourceTask boundedSourceTask) throws Exception {
    final String physicalTaskId = getPhysicalTaskId(boundedSourceTask.getId());
    final Map<String, Object> metric = new HashMap<>();
    metricCollector.beginMeasurement(physicalTaskId, metric);

    final long readStartTime = System.currentTimeMillis();
    final Readable readable = boundedSourceTask.getReadable();
    final Iterable readData = readable.read();
    final long readEndTime = System.currentTimeMillis();
    metric.put("BoundedSourceReadTime(ms)", readEndTime - readStartTime);

    final List<Long> writtenBytesList = new ArrayList<>();
    for (final OutputWriter outputWriter : physicalTaskIdToOutputWriterMap.get(physicalTaskId)) {
      outputWriter.write(readData);
      outputWriter.close();
      final Optional<Long> writtenBytes = outputWriter.getWrittenBytes();
      writtenBytes.ifPresent(writtenBytesList::add);
    }
    final long writeEndTime = System.currentTimeMillis();
    metric.put("OutputWriteTime(ms)", writeEndTime - readEndTime);
    putWrittenBytesMetric(writtenBytesList, metric);
    metricCollector.endMeasurement(physicalTaskId, metric);
  }

  /**
   * Processes an OperatorTask.
   * @param operatorTask to execute
   */
  private void launchOperatorTask(final OperatorTask operatorTask) {
    final Map<Transform, Object> sideInputMap = new HashMap<>();
    final String physicalTaskId = getPhysicalTaskId(operatorTask.getId());
    final Map<String, Object> metric = new HashMap<>();
    metricCollector.beginMeasurement(physicalTaskId, metric);

    final long readStartTime = System.currentTimeMillis();
    // Check for side inputs
    physicalTaskIdToInputReaderMap.get(physicalTaskId).stream().filter(InputReader::isSideInputReader)
        .forEach(inputReader -> {
          try {
            final Object sideInput = inputReader.getSideInput();
            final RuntimeEdge inEdge = inputReader.getRuntimeEdge();
            final Transform srcTransform;
            if (inEdge instanceof PhysicalStageEdge) {
              srcTransform = ((OperatorVertex) ((PhysicalStageEdge) inEdge).getSrcVertex())
                  .getTransform();
            } else {
              srcTransform = ((OperatorTask) inEdge.getSrc()).getTransform();
            }
            sideInputMap.put(srcTransform, sideInput);
          } catch (final InterruptedException | ExecutionException e) {
            throw new BlockFetchException(e);
          }
        });

    final Transform.Context transformContext = new ContextImpl(sideInputMap);
    final OutputCollectorImpl outputCollector = new OutputCollectorImpl();

    final Transform transform = operatorTask.getTransform();
    transform.prepare(transformContext, outputCollector);

    // Check for non-side inputs
    // This blocking queue contains the pairs having data and source vertex ids.
    final BlockingQueue<Pair<Iterator, String>> dataQueue = new LinkedBlockingQueue<>();
    final AtomicInteger sourceParallelism = new AtomicInteger(0);
    physicalTaskIdToInputReaderMap.get(physicalTaskId).stream().filter(inputReader -> !inputReader.isSideInputReader())
        .forEach(inputReader -> {
          final List<CompletableFuture<Iterator>> futures = inputReader.read();
          final String srcIrVtxId = inputReader.getSrcIrVertexId();
          sourceParallelism.getAndAdd(inputReader.getSourceParallelism());
          // Add consumers which will push the data to the data queue when it ready to the futures.
          futures.forEach(compFuture -> compFuture.whenComplete((data, exception) -> {
            if (exception != null) {
              throw new BlockFetchException(exception);
            }
            dataQueue.add(Pair.of(data, srcIrVtxId));
          }));
        });
    final long readFutureEndTime = System.currentTimeMillis();

    long accumulatedBlockedReadTime = 0;
    long accumulatedWriteTime = 0;
    // Consumes all of the partitions from incoming edges.
    for (int srcTaskNum = 0; srcTaskNum < sourceParallelism.get(); srcTaskNum++) {
      try {
        // Because the data queue is a blocking queue, we may need to wait some available data to be pushed.
        final long blockedReadStartTime = System.currentTimeMillis();
        final Pair<Iterator, String> availableData = dataQueue.take();
        final long blockedReadEndTime = System.currentTimeMillis();
        accumulatedBlockedReadTime += blockedReadEndTime - blockedReadStartTime;
        transform.onData(availableData.left(), availableData.right());
      } catch (final InterruptedException e) {
        throw new BlockFetchException(e);
      }

      // Check whether there is any output data from the transform and write the output of this task to the writer.
      final List output = outputCollector.collectOutputList();
      if (!output.isEmpty() && physicalTaskIdToOutputWriterMap.containsKey(physicalTaskId)) {
        final long writeStartTime = System.currentTimeMillis();
        physicalTaskIdToOutputWriterMap.get(physicalTaskId).forEach(outputWriter -> outputWriter.write(output));
        final long writeEndTime = System.currentTimeMillis();
        accumulatedWriteTime += writeEndTime - writeStartTime;
      } // If else, this is a sink task.
    }
    transform.close();

    metric.put("InputReadTime(ms)", readFutureEndTime - readStartTime + accumulatedBlockedReadTime);
    final long transformEndTime = System.currentTimeMillis();
    metric.put("TransformTime(ms)",
        transformEndTime - readFutureEndTime - accumulatedWriteTime - accumulatedBlockedReadTime);

    // Check whether there is any output data from the transform and write the output of this task to the writer.
    final List<Long> writtenBytesList = new ArrayList<>();
    final List output = outputCollector.collectOutputList();
    if (physicalTaskIdToOutputWriterMap.containsKey(physicalTaskId)) {
      for (final OutputWriter outputWriter : physicalTaskIdToOutputWriterMap.get(physicalTaskId)) {
        if (!output.isEmpty()) {
          outputWriter.write(output);
        }
        outputWriter.close();
        final Optional<Long> writtenBytes = outputWriter.getWrittenBytes();
        writtenBytes.ifPresent(writtenBytesList::add);
      }
    } else {
      LOG.info("This is a sink task: {}", physicalTaskId);
    }
    final long writeEndTime = System.currentTimeMillis();
    metric.put("OutputTime(ms)", writeEndTime - transformEndTime + accumulatedWriteTime);
    putWrittenBytesMetric(writtenBytesList, metric);

    metricCollector.endMeasurement(physicalTaskId, metric);
  }

  /**
   * Pass on the data to the following tasks.
   * @param task the task to carry on the data.
   */
  private void launchMetricCollectionBarrierTask(final MetricCollectionBarrierTask task) {
    final String physicalTaskId = getPhysicalTaskId(task.getId());
    final Map<String, Object> metric = new HashMap<>();
    metricCollector.beginMeasurement(physicalTaskId, metric);

    final long readStartTime = System.currentTimeMillis();
    final BlockingQueue<Iterator> dataQueue = new LinkedBlockingQueue<>();
    final AtomicInteger sourceParallelism = new AtomicInteger(0);
    physicalTaskIdToInputReaderMap.get(physicalTaskId).stream().filter(inputReader -> !inputReader.isSideInputReader())
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
    final long readEndTime = System.currentTimeMillis();
    metric.put("InputReadTime(ms)", readEndTime - readStartTime);

    final List<Long> writtenBytesList = new ArrayList<>();
    for (final OutputWriter outputWriter : physicalTaskIdToOutputWriterMap.get(physicalTaskId)) {
      outputWriter.write(data);
      outputWriter.close();
      final Optional<Long> writtenBytes = outputWriter.getWrittenBytes();
      writtenBytes.ifPresent(writtenBytesList::add);
    }
    final long writeEndTime  = System.currentTimeMillis();
    metric.put("OutputWriteTime(ms)", writeEndTime - readEndTime);
    putWrittenBytesMetric(writtenBytesList, metric);
    metricCollector.endMeasurement(physicalTaskId, metric);
  }

  /**
   * @param logicalTaskId the logical task id.
   * @return the physical task id.
   */
  private String getPhysicalTaskId(final String logicalTaskId) {
    return RuntimeIdGenerator.generatePhysicalTaskId(taskGroupIdx, logicalTaskId);
  }

  /**
   * Puts written bytes metric if the output data size is known.
   *
   * @param writtenBytesList the list of written bytes.
   * @param metricMap        the metric map to put written bytes metric.
   */
  private void putWrittenBytesMetric(final List<Long> writtenBytesList,
                                     final Map<String, Object> metricMap) {
    if (!writtenBytesList.isEmpty()) {
      long totalWrittenBytes = 0;
      for (final Long writtenBytes : writtenBytesList) {
        totalWrittenBytes += writtenBytes;
      }
      metricMap.put("WrittenBytes", totalWrittenBytes);
    }
  }
}
