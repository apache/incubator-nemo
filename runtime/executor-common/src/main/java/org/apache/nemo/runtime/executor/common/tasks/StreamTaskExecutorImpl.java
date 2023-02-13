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
package org.apache.nemo.runtime.executor.common.tasks;

import io.netty.buffer.ByteBuf;
import org.apache.nemo.common.*;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.common.punctuation.WatermarkWithIndex;
import org.apache.nemo.offloading.common.StateStore;
import org.apache.nemo.offloading.common.TaskHandlingEvent;
import org.apache.nemo.runtime.executor.common.*;
import org.apache.nemo.runtime.executor.common.datatransfer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Executes a task.
 * Should be accessed by a single thread.
 */
@NotThreadSafe
public final class StreamTaskExecutorImpl implements TaskExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(StreamTaskExecutorImpl.class.getName());

  // Essential information
  private final Task task;
  private final String taskId;
  private final List<SourceVertexDataFetcher> sourceVertexDataFetchers;
  private final SerializerManager serializerManager;

  private final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag;

  // Variables for prepareOffloading - start
  private final Map<String, Pair<OperatorMetricCollector, OutputCollector>> vertexIdAndCollectorMap;
  private final Map<String, List<String>> taskOutgoingEdges;
  private final Map<String, NextIntraTaskOperatorInfo> operatorInfoMap = new HashMap<>();
  // Variables for prepareOffloading - end

  private final long adjustTime;

  final Map<String, Double> samplingMap;

  private boolean isStateless = true;

  private final String executorId;

  private final long threadId;

  private final List<DataFetcher> allFetchers = new ArrayList<>();

  private final ExecutorService prepareService;

  private final ExecutorThreadQueue executorThreadQueue;

  private final AtomicBoolean prepared = new AtomicBoolean(false);

  private List<Transform> statefulTransforms;

  private final TaskMetrics taskMetrics;

  private final StateStore stateStore;

  private final InputPipeRegister inputPipeRegister;

  // private final OffloadingManager offloadingManager;

  private final PipeManagerWorker pipeManagerWorker;

  private final Map<String, DataFetcher> edgeToDataFetcherMap = new HashMap<>();

  private final boolean offloaded;

  private final RuntimeEdge outputEdge;

  private final Serializer serializer;

  private final String dstTaskId;

  private final WatermarkTracker taskWatermarkManager;

  private final int taskIndex;

  private final boolean singleOneToOneInput;
  private final IntermediateDataIOFactory intermediateDataIOFactory;

  private final DataHandler dataHandler;

  /**
   * Constructor.
   *
   * @param task                   Task with information needed during execution.
   * @param irVertexDag            A DAG of vertices.
   * @param intermediateDataIOFactory    For reading from/writing to data to other tasks.
   */
  public StreamTaskExecutorImpl(final long threadId,
                                final String executorId,
                                final Task task,
                                final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
                                final IntermediateDataIOFactory intermediateDataIOFactory,
                                final SerializerManager serializerManager,
                                final Map<String, Double> samplingMap,
                                final ExecutorService prepareService,
                                final ExecutorThreadQueue executorThreadQueue,
                                final InputPipeRegister inputPipeRegister,
                                final StateStore stateStore,
                                final PipeManagerWorker pipeManagerWorker,
                                final OutputCollectorGenerator outputCollectorGenerator,
                                final byte[] bytes,
                                final boolean offloaded) {
    // Essential information
    //LOG.info("Non-copied outgoing edges: {}", task.getTaskOutgoingEdges());
    this.offloaded = offloaded;
    this.pipeManagerWorker = pipeManagerWorker;
    this.intermediateDataIOFactory = intermediateDataIOFactory;
    // this.offloadingManager = offloadingManager;
    this.stateStore = stateStore;
    this.taskMetrics = new TaskMetrics();
    this.executorThreadQueue = executorThreadQueue;
    //LOG.info("Copied outgoing edges: {}, bytes: {}", copyOutgoingEdges);
    this.prepareService = prepareService;
    this.inputPipeRegister = inputPipeRegister;
    this.taskId = task.getTaskId();
    this.serializer = serializerManager
      .getSerializer(((RuntimeEdge)task.getTaskOutgoingEdges().get(0)).getId());

    this.statefulTransforms = new ArrayList<>();

    this.threadId = threadId;
    this.executorId = executorId;
    this.sourceVertexDataFetchers = new ArrayList<>();
    this.task = task;
    this.irVertexDag = irVertexDag;
    this.vertexIdAndCollectorMap = new HashMap<>();
    this.taskOutgoingEdges = new HashMap<>();
    this.samplingMap = samplingMap;
    this.outputEdge = task.getTaskOutgoingEdges().get(0);

    this.singleOneToOneInput = task.getTaskIncomingEdges().size() == 1
      && (task.getTaskIncomingEdges().get(0).getDataCommunicationPattern()
      .equals(CommunicationPatternProperty.Value.OneToOne) ||
      task.getTaskIncomingEdges().get(0).getDataCommunicationPattern()
      .equals(CommunicationPatternProperty.Value.TransientOneToOne));

    this.taskWatermarkManager = getTaskWatermarkManager();
    if (singleOneToOneInput) {
      this.dataHandler = new SingleO2ODataHandler();
    } else {
      this.dataHandler = new MultiInputDataHandler();
    }

    this.taskIndex = RuntimeIdManager.getIndexFromTaskId(taskId);
    this.dstTaskId = RuntimeIdManager.generateTaskId(((StageEdge)outputEdge).getDst().getId(),
      RuntimeIdManager.getIndexFromTaskId(taskId), 0);
    this.serializerManager = serializerManager;
    this.adjustTime = System.currentTimeMillis() - 1436918400000L;

    prepare();
  }

  private WatermarkTracker getTaskWatermarkManager() {
    if (task.getTaskIncomingEdges().size() == 0) {
      return null;
    } else if (task.getTaskIncomingEdges().size() == 1) {
      final int parallelism = task.getTaskIncomingEdges().get(0)
        .getSrcIRVertex().getPropertyValue(ParallelismProperty.class).get();

      LOG.info("Registering pipe for input edges {} in {}, parallelism {}",
        task.getTaskIncomingEdges().get(0).getId(), taskId, parallelism);

      final CommunicationPatternProperty.Value comm = task.getTaskIncomingEdges().get(0)
        .getDataCommunicationPattern();
      return new SingleStageWatermarkTracker(getWatermarkParllelism(parallelism, comm));

    } else if (task.getTaskIncomingEdges().size() == 2) {
      final StageEdge firstEdge = task.getTaskIncomingEdges().get(0);
      final StageEdge secondEdge = task.getTaskIncomingEdges().get(1);
      final int firstParallelism = ((StageEdge) firstEdge).getSrcIRVertex().getPropertyValue(ParallelismProperty.class)
        .get();
      final int secondParallelism = ((StageEdge) secondEdge).getSrcIRVertex().getPropertyValue(ParallelismProperty.class)
        .get();
      final CommunicationPatternProperty.Value firstComm = firstEdge
        .getDataCommunicationPattern();
       final CommunicationPatternProperty.Value secondComm = secondEdge
        .getDataCommunicationPattern();

       return new DoubleStageWatermarkTracker(
         firstEdge.getId(),
         getWatermarkParllelism(firstParallelism, firstComm),
         secondEdge.getId(),
         getWatermarkParllelism(secondParallelism, secondComm));
    } else {
      throw new RuntimeException("Not support incoming edge >= 3 " + taskId + ", " +  task.getTaskIncomingEdges());
    }
  }

  private int getWatermarkParllelism(final int parallelism,
                                     final CommunicationPatternProperty.Value comm) {
    if (comm.equals(CommunicationPatternProperty.Value.OneToOne)
      || comm.equals(CommunicationPatternProperty.Value.TransientOneToOne)) {
      return 1;
    } else {
      return parallelism;
    }
  }

  @Override
  public boolean isSourceAvailable() {
    return false;
  }

  // per second
  @Override
  public void setThrottleSourceRate(final long num) {
  }

  @Override
  public DefaultTaskExecutorImpl.CurrentState getStatus() {
    return null;
  }

  // For source task
  @Override
  public boolean hasData() {
    throw new RuntimeException("Not supported");
  }

  @Override
  public Task getTask() {
    return task;
  }

  public TaskMetrics getTaskMetrics() {
    return taskMetrics;
  }

  @Override
  public int getNumKeys() {
    return 0;
  }

  @Override
  public boolean isSource() {
    return false;
  }

  @Override
  public boolean isOffloadedTask() {
    return offloaded;
  }


  @Override
  public long getThreadId() {
    return threadId;
  }

  @Override
  public String getId() {
    return taskId;
  }

  @Override
  public boolean isStateless() {
    return true;
  }


  @Override
  public void initialize() {
    TaskExecutorUtil.sendInitMessage(task, inputPipeRegister);
  }

  private void prepare() {

    final long st = System.currentTimeMillis();

    LOG.info("Start to registering input output pipe {}", taskId);

    task.getTaskOutgoingEdges().forEach(edge -> {
      LOG.info("Task outgoing edge for {} {}", taskId, edge);
      final IRVertex src = edge.getSrcIRVertex();
      final IRVertex dst = edge.getDstIRVertex();
      taskOutgoingEdges.putIfAbsent(src.getId(), new LinkedList<>());
      taskOutgoingEdges.get(src.getId()).add(dst.getId());
      final Integer taskIndex = RuntimeIdManager.getIndexFromTaskId(task.getTaskId());

      // bidrectional !!
      final int parallelism = edge
        .getDstIRVertex().getPropertyValue(ParallelismProperty.class).get();

      final CommunicationPatternProperty.Value comm =
        edge.getPropertyValue(CommunicationPatternProperty.class).get();

      LOG.info("Registering pipe for output edges in {}, parallelism {}", taskId, parallelism);
      final List<String> dstTaskIds = TaskExecutorUtil.getDstTaskIds(taskId, edge);
      dstTaskIds.forEach(dstTaskId -> {
        inputPipeRegister.registerInputPipe(
          dstTaskId,
          edge.getId(),
          task.getTaskId(),
          new PipeInputReader(edge.getDstIRVertex(), taskId, (RuntimeEdge) edge,
            serializerManager.getSerializer(((RuntimeEdge)edge).getId()), executorThreadQueue));
      });

      LOG.info("End of task outgoing edge for {} {}", taskId, edge);
    });

    LOG.info("Task {} registering pipe time: {}", taskId, System.currentTimeMillis() - st);

    final int taskIndex = RuntimeIdManager.getIndexFromTaskId(task.getTaskId());

    // Traverse in a reverse-topological order to ensure that each visited vertex's children vertices exist.
    final List<IRVertex> reverseTopologicallySorted = new ArrayList<>(irVertexDag.getTopologicalSort());
    Collections.reverse(reverseTopologicallySorted);

    // Build a map for edge as a key and edge index as a value
    // This variable is used for creating NextIntraTaskOperatorInfo
    // in {@link this#getInternalMainOutputs and this#internalMainOutputs}
    reverseTopologicallySorted.forEach(childVertex -> {
      if (irVertexDag.getOutgoingEdgesOf(childVertex.getId()).size() == 0) {
        childVertex.isSink = true;

        /*
        // If it is sink or emit to next stage, we log the latency
        LOG.info("MonitoringVertex: {}", childVertex.getId());
        if (!samplingMap.containsKey(childVertex.getId())) {
          samplingMap.put(childVertex.getId(), 1.0);
        }
        */

        LOG.info("Sink vertex: {}", childVertex.getId());
      }
    });


    // serializedDag = SerializationUtils.serialize(irVertexDag);
    final Map<String, OutputCollector> outputCollectorMap = new HashMap<>();
    LOG.info("End of source vertex prepare {}", taskId);

      // Parent-task read
      // TODO #285: Cache broadcasted data
      task.getTaskIncomingEdges()
        .stream()
        // .filter(inEdge -> inEdge.getDstIRVertex().getId().equals(irVertex.getId())) // edge to this vertex
        .map(incomingEdge -> {

          LOG.info("Incoming edge: {}, taskIndex: {}, taskId: {}", incomingEdge, taskIndex, taskId);
          return Pair.of(incomingEdge, intermediateDataIOFactory
            .createReader(
              taskId,
              incomingEdge.getSrcIRVertex(), incomingEdge, executorThreadQueue));
        })
        .forEach(pair -> {
          final String irVertexId = pair.left().getDstIRVertex().getId();
          final IRVertex irVertex = irVertexDag.getVertexById(irVertexId);

          if (irVertex instanceof OperatorVertex) {

            // LOG.info("Adding data fetcher for {} / {}", taskId, irVertex.getId());

            final StageEdge edge = pair.left();
            final InputReader parentTaskReader = pair.right();
            final OutputCollector dataFetcherOutputCollector =
              new DataFetcherOutputCollector(edge.getSrcIRVertex(), (OperatorVertex) irVertex,
                outputCollectorMap.get(irVertex.getId()), taskId);

            final int parallelism = edge
              .getSrcIRVertex().getPropertyValue(ParallelismProperty.class).get();

            final CommunicationPatternProperty.Value comm =
              edge.getPropertyValue(CommunicationPatternProperty.class).get();

            final DataFetcher df = new MultiThreadParentTaskDataFetcher(
              taskId,
              edge.getSrcIRVertex(),
              edge,
              dataFetcherOutputCollector);

            edgeToDataFetcherMap.put(edge.getId(), df);

            // LOG.info("Adding data fetcher 22 for {} / {}, parallelism {}",
            //  taskId, irVertex.getId(), parallelism);

            LOG.info("Registering pipe for input edges in {}, parallelism {}", taskId, parallelism);

            TaskExecutorUtil.getSrcTaskIds(taskId, edge).forEach(srcTaskId -> {
              inputPipeRegister.registerInputPipe(
                srcTaskId,
                edge.getId(),
                task.getTaskId(),
                parentTaskReader);
            });

            allFetchers.add(df);

            // LOG.info("End of adding data fetcher for {} / {}", taskId, irVertex.getId());
          }
        });
    // return sortedHarnessList;


    LOG.info("Task {} prepar time: {}", taskId, System.currentTimeMillis() - st);
    prepared.set(true);

    LOG.info("Source vertex data fetchers in defaultTaskExecutorimpl: {}", sourceVertexDataFetchers);

    // For latency logging
    for (final Pair<OperatorMetricCollector, OutputCollector> metricCollector :
      vertexIdAndCollectorMap.values()) {
      metricCollector.left().setAdjustTime(adjustTime);
    }
  }

  @Override
  public void handleData(final String edgeId,
                         final TaskHandlingEvent taskHandlingEvent) {
    if (taskHandlingEvent instanceof TaskHandlingDataEvent) {
      final ByteBuf data = taskHandlingEvent.getDataByteBuf();
      dataHandler.handleRemoteByteBuf(data, taskHandlingEvent);
    } else if (taskHandlingEvent instanceof TaskLocalDataEvent) {
      final Object data = taskHandlingEvent.getData();
      dataHandler.handleLocalData(data, taskHandlingEvent);
    }
  }

  // exeutor thread가 바로 부르는 method
  @Override
  public boolean handleSourceData() {
    throw new RuntimeException("Not supported");
  }


  ////////////////////////////////////////////// Transform-specific helper methods

  private Optional<TaskInputWatermarkManager> restoreTaskInputWatermarkManager() {
    if (stateStore.containsState(taskId + "-taskWatermarkManager")) {
      try {
        final InputStream is = stateStore.getStateStream(taskId + "-taskWatermarkManager");
        final TaskInputWatermarkManager tm = TaskInputWatermarkManager.decode(taskId, is);
        return Optional.of(tm);
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
    return Optional.empty();
  }

  @Override
  public void restore() {
  }

  public void setIRVertexPutOnHold(final IRVertex irVertex) {
  }

  @Override
  public boolean checkpoint(final boolean checkpointSource, final String checkpointId) {
    boolean hasChekpoint = false;

    final DataOutputStream os = new DataOutputStream(
      stateStore.getOutputStream(checkpointId + "-taskWatermarkManager"));

    try {
      if (task.getTaskIncomingEdges().size() == 0) {
        // do nothing
      } else if (task.getTaskIncomingEdges().size() == 1) {
        ((SingleStageWatermarkTracker)taskWatermarkManager).encode(taskId, os);
      } else if (task.getTaskIncomingEdges().size() == 2) {
        ((DoubleStageWatermarkTracker)taskWatermarkManager).encode(taskId, os);
      } else {
        throw new RuntimeException("Not supported edge > 2" + taskId);
      }
      os.close();
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    return true;
  }

  interface DataHandler {
    void handleLocalData(Object data, TaskHandlingEvent event);
    void handleRemoteByteBuf(ByteBuf data,
                             TaskHandlingEvent event);
  }

  final class SingleO2ODataHandler implements DataHandler {

    @Override
    public void handleLocalData(Object data, TaskHandlingEvent event) {
      pipeManagerWorker.writeData(taskId, outputEdge.getId(), dstTaskId, serializer, data);
    }

    @Override
    public void handleRemoteByteBuf(ByteBuf data, TaskHandlingEvent event) {
      pipeManagerWorker.writeByteBufData(taskId, outputEdge.getId(), dstTaskId, data);
    }
  }

  final class MultiInputDataHandler implements DataHandler {

    @Override
    public void handleLocalData(Object data, TaskHandlingEvent event) {
      if (data instanceof WatermarkWithIndex) {
        // watermark!
        // we should manage the watermark
        final WatermarkWithIndex watermarkWithIndex = (WatermarkWithIndex) data;
        taskWatermarkManager.trackAndEmitWatermarks(taskId,
          event.getEdgeId(),
          watermarkWithIndex.getIndex(),
          watermarkWithIndex.getWatermark().getTimestamp())
          .ifPresent(watermark -> {
            pipeManagerWorker.writeData(taskId, outputEdge.getId(),
              dstTaskId,
              serializerManager.getSerializer(event.getEdgeId()),
              new WatermarkWithIndex(new Watermark(watermark), taskIndex));
          });

      } else {
        pipeManagerWorker.writeData(taskId, outputEdge.getId(), dstTaskId, serializer, data);
      }
    }

    @Override
    public void handleRemoteByteBuf(ByteBuf data, TaskHandlingEvent event) {
      final ByteBuf byteBuf = data;
      byteBuf.markReaderIndex();
      final Byte b = byteBuf.readByte();
      byteBuf.resetReaderIndex();

      if (b == 0x01) {
        // watermark!
        // we should manage the watermark
        final WatermarkWithIndex watermarkWithIndex = (WatermarkWithIndex) event.getData();
        taskWatermarkManager.trackAndEmitWatermarks(taskId,
          event.getEdgeId(), watermarkWithIndex.getIndex(),
          watermarkWithIndex.getWatermark().getTimestamp())
          .ifPresent(watermark -> {

            // LOG.info("Emit watermark streamvertex in {} {}", taskId, new Instant(watermark.getTimestamp()));

            pipeManagerWorker.writeData(taskId, outputEdge.getId(),
              dstTaskId,
              serializerManager.getSerializer(event.getEdgeId()),
              new WatermarkWithIndex(new Watermark(watermark), taskIndex));
          });
      } else {
        pipeManagerWorker.writeByteBufData(taskId, outputEdge.getId(), dstTaskId, data);
      }
    }
  }

  @Override
  public String toString() {
    return taskId;
  }
}