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
import org.apache.commons.lang3.tuple.Triple;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.Task;
import org.apache.nemo.common.TaskMetrics;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.PairEdgeProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.punctuation.WatermarkWithIndex;
import org.apache.nemo.offloading.common.ServerlessExecutorProvider;
import org.apache.nemo.offloading.common.StateStore;
import org.apache.nemo.offloading.common.TaskHandlingEvent;
import org.apache.nemo.runtime.executor.common.*;
import org.apache.nemo.runtime.executor.common.datatransfer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.nemo.runtime.executor.common.TaskExecutorUtil.getDstTaskIds;

/**
 * Executes a task.
 * Should be accessed by a single thread.
 * TODO: Watermark tracker로 인해 hop이 커지면 latency가 증가함. 그냥 watermark progress input에서만?
 * TODO: 아니면 shuffle edge만!
 */
@NotThreadSafe
public final class SingleO2OOutputR3CRTaskExecutorImpl implements CRTaskExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(SingleO2OOutputR3CRTaskExecutorImpl.class.getName());

  // Essential information
  private final Task task;
  private final String taskId;
  private final List<SourceVertexDataFetcher> sourceVertexDataFetchers;
  private final SerializerManager serializerManager;

  private final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag;

  // Variables for prepareOffloading - start
  private final ServerlessExecutorProvider serverlessExecutorProvider;
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

  private R2WatermarkManager taskWatermarkManager;
  private final InputPipeRegister inputPipeRegister;

  // private final OffloadingManager offloadingManager;

  private final PipeManagerWorker pipeManagerWorker;

  private final Map<String, DataFetcher> edgeToDataFetcherMap = new HashMap<>();

  private final OutputCollectorGenerator outputCollectorGenerator;

  private final boolean offloaded;

  private final Transform.ConditionalRouting conditionalRouting;

  private final boolean singleOneToOneInput;
  private final boolean singleOneToOneOutput;

  final Map<String, List<OutputWriter>> externalAdditionalOutputMap;

  // THIS TASK SHOULD NOT BE MOVED !!
  // THIS TASK SHOULD NOT BE MOVED !!
  // THIS TASK SHOULD NOT BE MOVED !!

  private final RuntimeEdge transientPathEdge;
  private final Serializer transientPathSerializer;

  private final RuntimeEdge vmPathEdge;
  private final Serializer vmPathSerializer;

  private final IntermediateDataIOFactory intermediateDataIOFactory;

  private final GetDstTaskId getDstTaskId;

  private final int taskIndex;

  private final String vmPathDstTask;
  private final String transientPathDstTask;
  private boolean dataRerouting;
  private boolean watermarkRerouting;
  private DataRouter dataRouter;
  private DataRouter watermarkRouter;
  private final DataHandler dataHandler;

  /**
   * Constructor.
   *
   * @param task                   Task with information needed during execution.
   * @param irVertexDag            A DAG of vertices.
   * @param intermediateDataIOFactory    For reading from/writing to data to other tasks.
   */
  public SingleO2OOutputR3CRTaskExecutorImpl(final long threadId,
                                             final String executorId,
                                             final Task task,
                                             final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
                                             final IntermediateDataIOFactory intermediateDataIOFactory,
                                             final SerializerManager serializerManager,
                                             final ServerlessExecutorProvider serverlessExecutorProvider,
                                             final Map<String, Double> samplingMap,
                                             final boolean isLocalSource,
                                             final ExecutorService prepareService,
                                             final ExecutorThreadQueue executorThreadQueue,
                                             final InputPipeRegister inputPipeRegister,
                                             final StateStore stateStore,
                                             //  final OffloadingManager offloadingManager,
                                             final PipeManagerWorker pipeManagerWorker,
                                             final OutputCollectorGenerator outputCollectorGenerator,
                                             final byte[] bytes,
                                             final Transform.ConditionalRouting conditionalRouting,
                                             // final OffloadingPreparer offloadingPreparer,
                                             final boolean offloaded) {
    // Essential information
    //LOG.info("Non-copied outgoing edges: {}", task.getTaskOutgoingEdges());
    this.offloaded = offloaded;
    this.intermediateDataIOFactory = intermediateDataIOFactory;
    this.conditionalRouting = conditionalRouting;
    this.outputCollectorGenerator = outputCollectorGenerator;
    this.pipeManagerWorker = pipeManagerWorker;
    // this.offloadingManager = offloadingManager;
    this.stateStore = stateStore;
    this.taskMetrics = new TaskMetrics();
    this.executorThreadQueue = executorThreadQueue;
    //LOG.info("Copied outgoing edges: {}, bytes: {}", copyOutgoingEdges);
    this.prepareService = prepareService;
    this.inputPipeRegister = inputPipeRegister;
    this.taskId = task.getTaskId();
    this.taskIndex = RuntimeIdManager.getIndexFromTaskId(taskId);

    this.externalAdditionalOutputMap = new HashMap<>();

    this.statefulTransforms = new ArrayList<>();

    final long restoresSt = System.currentTimeMillis();

    this.singleOneToOneInput = task.getTaskIncomingEdges().size() == 1
      && (task.getTaskIncomingEdges().get(0).getDataCommunicationPattern()
      .equals(CommunicationPatternProperty.Value.OneToOne) ||
      task.getTaskIncomingEdges().get(0).getDataCommunicationPattern()
        .equals(CommunicationPatternProperty.Value.TransientOneToOne));

    this.singleOneToOneOutput = task.getTaskOutgoingEdges().get(0)
      .getDataCommunicationPattern()
      .equals(CommunicationPatternProperty.Value.OneToOne) ||
      task.getTaskOutgoingEdges().get(0)
      .getDataCommunicationPattern()
      .equals(CommunicationPatternProperty.Value.TransientOneToOne);

    LOG.info("Task {} watermark manager restore time {}", taskId, System.currentTimeMillis() - restoresSt);

    this.threadId = threadId;
    this.executorId = executorId;
    this.sourceVertexDataFetchers = new ArrayList<>();
    this.task = task;
    this.irVertexDag = irVertexDag;
    this.vertexIdAndCollectorMap = new HashMap<>();
    this.taskOutgoingEdges = new HashMap<>();
    this.samplingMap = samplingMap;
    // samplingMap.putAll(evalConf.samplingJson);

    this.serverlessExecutorProvider = serverlessExecutorProvider;

    this.serializerManager = serializerManager;

    this.transientPathEdge = task.getTaskOutgoingEdges().stream().filter(edge ->
      edge.isTransientPath())
      .findFirst().get();

    final List<String> transientDsts = getDstTaskIds(taskId, transientPathEdge);
    if (transientDsts.size() > 1) {
      throw new RuntimeException("Invalid dst size " + transientDsts + " in " + taskId);
    }

    this.transientPathDstTask = transientDsts.get(0);
    this.transientPathSerializer = serializerManager.getSerializer(transientPathEdge.getId());

    this.vmPathEdge = task.getTaskOutgoingEdges().stream().filter(edge ->
      !edge.isTransientPath())
      .findFirst().get();

    final List<String> dstTasks = getDstTaskIds(taskId, vmPathEdge);
    this.vmPathDstTask = dstTasks.get(0);
    this.vmPathSerializer = serializerManager.getSerializer(vmPathEdge.getId());

    if (task.getTaskIncomingEdges().size() > 2) {
      this.taskWatermarkManager = new R2MultiPairWatermarkManager(taskId);
    } else {
      this.taskWatermarkManager = new R2SinglePairWatermarkManager(taskId);
    }

    LOG.info("VM path dst task {} transientPathTask {}, vmEdge {}, transientEdge {} in {}",
      vmPathDstTask, transientPathDstTask,
      vmPathEdge.getId(),
      transientPathEdge.getId(),
      taskId);

    this.getDstTaskId = new O2oDstTaskId();
    this.dataRerouting = false;
    this.watermarkRerouting = false;
    this.dataRouter = new VMDataRouter(vmPathDstTask);
    this.watermarkRouter = new VMDataRouter(vmPathDstTask);

    if (isLocalSource) {
      this.adjustTime = System.currentTimeMillis() - 1436918400000L;
    } else {
      this.adjustTime = 0;
    }

    this.dataHandler = new MultiInputSingleOutputDataHandler();

    prepare();
  }

  @Override
  public boolean isSourceAvailable() {
    throw new RuntimeException("not supported");
  }


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
    return isStateless;
  }

  @Override
  public void initialize() {
    TaskExecutorUtil.sendInitMessage(task, inputPipeRegister);
  }

  @Override
  public void stopInputPipeIndex(final Triple<String, String, String> triple) {
    LOG.info("Stop input pipe index {}", triple);
    final int taskIndex = RuntimeIdManager.getIndexFromTaskId(triple.getLeft());
    final String edgeId = triple.getMiddle();
    taskWatermarkManager.stopIndex(taskIndex, edgeId);
  }

  @Override
  public void startInputPipeIndex(final Triple<String, String, String> triple) {
    LOG.info("Start input pipe index {}", triple);
    final int taskIndex = RuntimeIdManager.getIndexFromTaskId(triple.getLeft());
    final String edgeId = triple.getMiddle();
    taskWatermarkManager.startIndex(taskIndex, edgeId);
  }

  // R2
  @Override
  public void startAndStopInputPipeIndex(final Triple<String, String, String> triple) {
    LOG.info("Start and stop input pipe index {}", triple);
    final int taskIndex = RuntimeIdManager.getIndexFromTaskId(triple.getLeft());
    final String edgeId = triple.getMiddle();
    taskWatermarkManager.startAndStopPairIndex(taskIndex, edgeId);
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

      if (childVertex.isGBK || childVertex.isPushback) {
        isStateless = false;
        if (childVertex instanceof OperatorVertex) {
          final OperatorVertex ov = (OperatorVertex) childVertex;
          statefulTransforms.add(ov.getTransform());
          LOG.info("Set GBK final transform");
        }
      }

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

    // Create a harness for each vertex
    reverseTopologicallySorted.forEach(irVertex -> {
        final Optional<Readable> sourceReader = TaskExecutorUtil
          .getSourceVertexReader(irVertex, task.getIrVertexIdToReadable());

        if (sourceReader.isPresent() != irVertex instanceof SourceVertex) {
          throw new IllegalStateException(irVertex.toString());
        }

      externalAdditionalOutputMap.putAll(
        TaskExecutorUtil.getExternalAdditionalOutputMap(
          irVertex, task.getTaskOutgoingEdges(), intermediateDataIOFactory, taskId,
          taskMetrics));

        final OutputCollector outputCollector = outputCollectorGenerator
          .generate(irVertex,
            taskId,
            irVertexDag,
            this,
            serializerManager,
            samplingMap,
            vertexIdAndCollectorMap,
            taskMetrics,
            task.getTaskOutgoingEdges(),
            operatorInfoMap,
            externalAdditionalOutputMap);

        outputCollectorMap.put(irVertex.getId(), outputCollector);

        // Create VERTEX HARNESS
        final Transform.Context context = new TransformContextImpl(
          irVertex, serverlessExecutorProvider, taskId, stateStore,
          conditionalRouting,
          executorId);

        TaskExecutorUtil.prepareTransform(irVertex, context, outputCollector, taskId);

        // Prepare data READ
        // Source read
        // TODO[SLS]: should consider multiple outgoing edges
        // All edges will have the same encoder/decoder!
        if (irVertex instanceof SourceVertex) {
          // final RuntimeEdge edge = irVertexDag.getOutgoingEdgesOf(irVertex).get(0);
          final RuntimeEdge edge = task.getTaskOutgoingEdges().get(0);
          // srcSerializer = serializerManager.getSerializer(edge.getId());
          // LOG.info("SourceVertex: {}, edge: {}, serializer: {}", irVertex.getId(), edge.getId(),
          // srcSerializer);

          // Source vertex read
          final SourceVertexDataFetcher fe = new SourceVertexDataFetcher(
            (SourceVertex) irVertex,
            edge,
            sourceReader.get(),
            outputCollector,
            prepareService,
            taskId,
            prepared,
            new Readable.ReadableContext() {
              @Override
              public StateStore getStateStore() {
                return stateStore;
              }

              @Override
              public String getTaskId() {
                return taskId;
              }
            },
            offloaded);

          edgeToDataFetcherMap.put(edge.getId(), fe);

          sourceVertexDataFetchers.add(fe);
          allFetchers.add(fe);

          if (sourceVertexDataFetchers.size() > 1) {
            throw new RuntimeException("Source vertex data fetcher is larger than one");
          }
        }
      });

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

            if (comm.equals(CommunicationPatternProperty.Value.OneToOne)
              || comm.equals(CommunicationPatternProperty.Value.TransientOneToOne)) {
              // LOG.info("Adding data fetcher 33 for {} / {}", taskId, irVertex.getId());
              if (edge.isTransientPath()) {
                final String pairVMEdgeId = edge.getPropertyValue(PairEdgeProperty.class).get();
                taskWatermarkManager.addDataFetcher(pairVMEdgeId, df.getEdgeId(), 1);
              } else {
                if (task.getTaskIncomingEdges().size() == 1) {
                  taskWatermarkManager.addDataFetcher(df.getEdgeId(), df.getEdgeId(), 1);
                }
              }

            } else {
              if (edge.isTransientPath()) {
                final String pairVMEdgeId = edge.getPropertyValue(PairEdgeProperty.class).get();
                taskWatermarkManager.addDataFetcher(pairVMEdgeId, df.getEdgeId(), parallelism);
              } else {
                if (task.getTaskIncomingEdges().size() == 1) {
                  taskWatermarkManager.addDataFetcher(df.getEdgeId(), df.getEdgeId(), parallelism);
                }
              }
              // LOG.info("Adding data fetcher 44 for {} / {}", taskId, irVertex.getId());
            }


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
  public void setRerouting(final String originTask,
                           final String pairTaskId,
                           final String pairEdgeId,
                           final ReroutingState state) {

    if (pairEdgeId.equals(transientPathEdge.getId())) {
      // rerouting from VM to Lambda
      dataRerouting = true;
      dataRouter = new LambdaDataRouter(transientPathDstTask);
      if (state.equals(ReroutingState.DATA_WATERMARK_BOTH)) {
        watermarkRerouting = true;
      } else {
        watermarkRerouting = false;
      }
      watermarkRouter = createWatermarkRouter();

    } else {
      // rerouting from Lambda to VM
      dataRerouting = false;
      dataRouter = new VMDataRouter(vmPathDstTask);
      if (state.equals(ReroutingState.DATA_WATERMARK_BOTH)) {
        watermarkRerouting = false;
      } else {
        watermarkRerouting = true;
      }
      watermarkRouter = createWatermarkRouter();
    }
  }

  private DataRouter createWatermarkRouter() {

    if (dataRerouting && watermarkRerouting) {
      return new LambdaDataRouter(transientPathDstTask);
    } else if (!dataRerouting && !watermarkRerouting) {
      return new VMDataRouter(vmPathDstTask);
    } else {
      return new VMLambdaBothRouter(transientPathDstTask, vmPathDstTask);
    }
  }

  @Override
  public void handleData(final String edgeId,
                         final TaskHandlingEvent taskHandlingEvent) {
    // watermark handling
    try {
      if (taskHandlingEvent instanceof TaskHandlingDataEvent) {
        final ByteBuf data = taskHandlingEvent.getDataByteBuf();
        dataHandler.handleRemoteByteBuf(data, taskHandlingEvent);
      } else if (taskHandlingEvent instanceof TaskLocalDataEvent) {
        final Object data = taskHandlingEvent.getData();
        dataHandler.handleLocalData(data, taskHandlingEvent);
      }
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Exception while processing data in " + taskId +
        " from " + edgeId + "/" + taskHandlingEvent.getTaskId());
    }
  }

  // exeutor thread가 바로 부르는 method
  @Override
  public boolean handleSourceData() {
    throw new RuntimeException("not support");
  }

  ////////////////////////////////////////////// Transform-specific helper methods


  public void setIRVertexPutOnHold(final IRVertex irVertex) {
  }

  private boolean finished = false;

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
    throw new RuntimeException("CRTask not support restore " + taskId);
    // stateRestore(taskId);
  }

  /*
  private void stateRestore(final String id) {
    try {
      final InputStream is = stateStore.getStateStream(id + "-taskWatermarkManager");
      taskWatermarkManager = TaskInputWatermarkManager.decode(is);
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    if (!isStateless) {
      statefulTransforms.forEach(transform -> transform.restore(id));
    }
  }

  private void stateMigration(final String id) {
    final OutputStream os = stateStore.getOutputStream(id + "-taskWatermarkManager");
    try {
      taskWatermarkManager.encode(os);
      os.close();
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    if (!isStateless) {
      statefulTransforms.forEach(transform -> transform.checkpoint(id));
    }

  }
  */

  @Override
  public boolean checkpoint(final boolean checkpointSource,
                            final String checkpointId) {
    throw new RuntimeException("CRTask not support checkpoint " + taskId);
    // stateMigration(checkpointId);
    // return true;
  }

  @Override
  public String toString() {
    return taskId;
  }

  interface GetDstTaskId {
    int getDstTaskIdIndex();
  }


  /////////////////////////////////////////////////////
  /////////////////////////////////////////////////////
  /////////////////////////////////////////////////////
  /////////////////////////////////////////////////////
  /////////////////////////////////////////////////////
  ////////////////////////////////////////////////////

  interface DataHandler {
    void handleLocalData(Object data, TaskHandlingEvent event);
    void handleRemoteByteBuf(ByteBuf data,
                             TaskHandlingEvent event);
  }

  final class MultiInputSingleOutputDataHandler implements DataHandler {

    @Override
    public void handleLocalData(Object data,
                                TaskHandlingEvent event) {
      if (data instanceof WatermarkWithIndex) {
        // watermark!
        // we should manage the watermark
        final WatermarkWithIndex watermarkWithIndex = (WatermarkWithIndex) data;
//        if (taskId.contains("Stage4")) {
//          LOG.info("Receive R3 CR watermark from {}/{} in {} {}",
//            event.getEdgeId(), watermarkWithIndex.getIndex(), taskId,
//            new Instant(((WatermarkWithIndex) data).getWatermark().getTimestamp()),
//            Thread.currentThread());
//        }
        taskWatermarkManager.updateWatermark(event.getEdgeId(), watermarkWithIndex.getIndex(),
          watermarkWithIndex.getWatermark().getTimestamp())
          .ifPresent(watermark -> {
            taskMetrics.setInputWatermark(watermark.getTimestamp());
            // LOG.info("Emit R3 CR watermark in {} {}", taskId, ((WatermarkWithIndex) data).getWatermark().getTimestamp());
            watermarkRouter.writeWatermark(new WatermarkWithIndex(watermark, taskIndex));
          });

      } else {
        // final long start = System.nanoTime();
        dataRouter.writeData(data, serializerManager.getSerializer(event.getEdgeId()));
        // final long et = System.nanoTime();
        // taskMetrics.incrementComputation(et - start);
      }
    }

    @Override
    public void handleRemoteByteBuf(ByteBuf data, TaskHandlingEvent taskHandlingEvent) {
      final ByteBuf byteBuf = data;
      byteBuf.markReaderIndex();
      final Byte b = byteBuf.readByte();
      byteBuf.resetReaderIndex();

      if (b == 0x01) {
        // watermark!
        // we should manage the watermark
        final WatermarkWithIndex watermarkWithIndex = (WatermarkWithIndex) taskHandlingEvent.getData();
//        if (taskId.contains("Stage4")) {
//          LOG.info("Receive R3 CR watermark from {}/{} in {} {} {}",
//            taskHandlingEvent.getEdgeId(), watermarkWithIndex.getIndex(), taskId,
//            new Instant(watermarkWithIndex.getWatermark().getTimestamp()),
//            Thread.currentThread());
//        }

        // LOG.info("Receive R3 CR watermark in {} {}", taskId, watermarkWithIndex.getWatermark().getTimestamp());
        taskWatermarkManager.updateWatermark(taskHandlingEvent.getEdgeId(), watermarkWithIndex.getIndex(),
          watermarkWithIndex.getWatermark().getTimestamp())
          .ifPresent(watermark -> {
            taskMetrics.setInputWatermark(watermark.getTimestamp());
            // LOG.info("Emit R3 CR watermark in {} {}", taskId, watermark.getTimestamp());
            watermarkRouter.writeWatermark(new WatermarkWithIndex(watermark, taskIndex));
          });
      } else {
        // data
        // final long start = System.nanoTime();
        dataRouter.writeByteBuf(data);
        // final long et = System.nanoTime();
        //taskMetrics.incrementComputation(et - start);
      }
    }
  }


  interface DataRouter {
    void writeWatermark(Object data);
    void writeData(Object data, Serializer serializer);
    void writeByteBuf(ByteBuf data);
  }


  final class VMLambdaBothRouter implements DataRouter {

    private final String lambdaId;
    private final String vmId;

    public VMLambdaBothRouter(final String lambdaId,
                                final String vmId) {
      this.lambdaId = lambdaId;
      this.vmId = vmId;
    }

    @Override
    public void writeByteBuf(final ByteBuf data) {
      // LOG.info("Emit both R3 CR watermark in {}->{}", taskId, lambdaId);
      // Lambda path
      pipeManagerWorker.writeByteBufData(taskId,
        transientPathEdge.getId(), lambdaId, data);

      // VM path
      pipeManagerWorker.writeByteBufData(taskId, vmPathEdge.getId(),
        vmId, data);
    }

    @Override
    public void writeWatermark(Object watermark) {
      // final WatermarkWithIndex w = new WatermarkWithIndex((Watermark) watermark, taskIndex);
      pipeManagerWorker.writeData(taskId, transientPathEdge.getId(),
        lambdaId,
        transientPathSerializer,
        watermark);

      pipeManagerWorker.writeData(taskId, vmPathEdge.getId(),
        vmId,
        vmPathSerializer,
        watermark);
    }

    @Override
    public void writeData(Object data, Serializer serializer) {
      pipeManagerWorker.writeData(taskId, transientPathEdge.getId(),
        lambdaId,
        serializer,
        data);

      pipeManagerWorker.writeData(taskId, vmPathEdge.getId(),
        vmId,
        serializer,
        data);
    }
  }

  final class LambdaDataRouter implements DataRouter {
    private final String lambdaTaskId;

    public LambdaDataRouter(final String lambdaTaskId) {
      this.lambdaTaskId = lambdaTaskId;
    }

    @Override
    public void writeWatermark(Object data) {
      pipeManagerWorker.writeData(taskId, transientPathEdge.getId(),
        lambdaTaskId,
        transientPathSerializer,
        data);
    }

    @Override
    public void writeData(Object data, Serializer serializer) {

//      LOG.info("Writing data from {} to {}/{}, serializer {}", taskId, transientPathEdge.getId(), lambdaTaskId,
//        serializer);

      pipeManagerWorker.writeData(taskId, transientPathEdge.getId(),
        lambdaTaskId,
        serializer,
        data);
    }

    @Override
    public void writeByteBuf(ByteBuf data) {
      pipeManagerWorker.writeByteBufData(taskId,
        transientPathEdge.getId(), lambdaTaskId, data);
    }
  }

  final class VMDataRouter implements DataRouter {
    private final String vmTaskId;

    public VMDataRouter(final String vmTaskId) {
      this.vmTaskId = vmTaskId;
    }

    @Override
    public void writeWatermark(Object data) {
      pipeManagerWorker.writeData(taskId, vmPathEdge.getId(),
        vmTaskId,
        vmPathSerializer,
        data);
    }

    @Override
    public void writeData(Object data, Serializer serializer) {
      pipeManagerWorker.writeData(taskId, vmPathEdge.getId(),
        vmTaskId,
        serializer,
        data);
    }

    @Override
    public void writeByteBuf(ByteBuf data) {
      pipeManagerWorker.writeByteBufData(taskId, vmPathEdge.getId(),
        vmTaskId, data);
    }
  }

  final class O2oDstTaskId implements GetDstTaskId {
    public O2oDstTaskId() {
    }

    @Override
    public int getDstTaskIdIndex() {
      return 0;
    }
  }
}
