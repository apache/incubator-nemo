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
import org.apache.nemo.common.punctuation.TimestampAndValue;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.common.punctuation.WatermarkWithIndex;
import org.apache.nemo.offloading.common.ServerlessExecutorProvider;
import org.apache.nemo.offloading.common.StateStore;
import org.apache.nemo.offloading.common.TaskHandlingEvent;
import org.apache.nemo.runtime.executor.common.*;
import org.apache.nemo.runtime.executor.common.datatransfer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.nemo.runtime.executor.common.TaskExecutorUtil.getDstTaskIds;

@NotThreadSafe
public final class MergerTaskExecutorImpl implements MergerTaskExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(MergerTaskExecutorImpl.class.getName());

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

  private final TaskMetrics taskMetrics;

  private final StateStore stateStore;

  private R2WatermarkManager taskWatermarkManager;
  private final InputPipeRegister inputPipeRegister;

  // private final OffloadingManager offloadingManager;

  private final PipeManagerWorker pipeManagerWorker;

  private final Map<String, DataFetcher> edgeToDataFetcherMap = new HashMap<>();

  private final OutputCollectorGenerator outputCollectorGenerator;

  private final boolean offloaded;

  final Map<String, List<OutputWriter>> externalAdditionalOutputMap;

  // THIS TASK SHOULD NOT BE MOVED !!
  // THIS TASK SHOULD NOT BE MOVED !!
  // THIS TASK SHOULD NOT BE MOVED !!

  private final RuntimeEdge transientOutputPathEdge;
  private final RuntimeEdge transientInputPathEdge;
  private final List<String> transientPathDstTasks;
  private final Serializer transientPathSerializer;

  private final RuntimeEdge vmOutputPathEdge;
  private final RuntimeEdge vmInputPathEdge;
  private final List<String> vmPathDstTasks;
  private final Serializer vmPathSerializer;

  private final IntermediateDataIOFactory intermediateDataIOFactory;

  private final GetDstTaskId getDstTaskId;

  private final int taskIndex;

  private final Transform mergerTransform;

  private final OutputCollector outputCollector;

  private final String vmPathDstTask;
  private final String transientPathDstTask;

  private DataRouter dataRouter;
  private DataHandler dataHandler;
  private boolean receiveFinal = true;

  /**
   * 무조건 single o2o (normal) - o2o (transient) 를 input으로 받음.
   * Output: single o2o (normal) - o2o (transient)
   */
  public MergerTaskExecutorImpl(final long threadId,
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

    final long restoresSt = System.currentTimeMillis();

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

    this.transientOutputPathEdge = task.getTaskOutgoingEdges().stream().filter(edge ->
      edge.isTransientPath())
      .findFirst().get();

    this.transientInputPathEdge = task.getTaskIncomingEdges().stream().filter(edge ->
      edge.isTransientPath())
      .findFirst().get();

    this.transientPathDstTasks = getDstTaskIds(taskId, transientOutputPathEdge);
    this.transientPathSerializer = serializerManager.getSerializer(transientOutputPathEdge.getId());

    this.vmOutputPathEdge = task.getTaskOutgoingEdges().stream().filter(edge ->
      !edge.isTransientPath())
      .findFirst().get();

     this.vmInputPathEdge = task.getTaskIncomingEdges().stream().filter(edge ->
      !edge.isTransientPath())
      .findFirst().get();

    this.vmPathDstTasks = getDstTaskIds(taskId, vmOutputPathEdge);
    this.vmPathSerializer = serializerManager.getSerializer(vmOutputPathEdge.getId());

    if (vmPathDstTasks.size() > 1) {
      throw new RuntimeException("Not supported multiple dstTAsks in merger task "
        + vmPathDstTasks + " , " + taskId);
    }

    this.getDstTaskId = new O2oDstTaskId();

    this.vmPathDstTask = vmPathDstTasks.get(0);
    this.transientPathDstTask = transientPathDstTasks.get(0);

    if (isLocalSource) {
      this.adjustTime = System.currentTimeMillis() - 1436918400000L;
    } else {
      this.adjustTime = 0;
    }

    this.dataRouter = new VMDataRouter(vmPathDstTask);
    this.dataHandler = new BypassDataHandler();

    // Here, we reset data router and data handler
    this.taskWatermarkManager = getOrRestoreState();

    this.mergerTransform = ((OperatorVertex)irVertexDag.getVertices().get(0)).getTransform();

    this.outputCollector = new OutputCollector() {
      private long ts;
      @Override
      public void setInputTimestamp(long timestamp) {
        ts = timestamp;
      }

      @Override
      public long getInputTimestamp() {
        return ts;
      }

      @Override
      public void emit(Object output) {
        taskMetrics.incrementOutputElement();
        final long timestamp = ts;
        dataRouter.writeData(new TimestampAndValue<>(timestamp, output));
      }

      @Override
      public void emitWatermark(Watermark watermark) {
        // LOG.info("SM vertex {} emits watermark {}", taskId, watermark.getTimestamp());
        dataRouter.writeData(new WatermarkWithIndex(watermark, taskIndex));
      }

      @Override
      public void emit(String dstVertexId, Object output) {
        throw new RuntimeException("Not support additional output in merger task " + taskId + ", " + dstVertexId);
      }
    };

    prepare();
  }

  private R2WatermarkManager getOrRestoreState() {
    if (stateStore.containsState(taskId + "-taskWatermarkManager")) {
      try (final DataInputStream is = new DataInputStream(
        stateStore.getStateStream(taskId + "-taskWatermarkManager"))) {
        final R2WatermarkManager manager;
        if (task.getTaskIncomingEdges().size() > 2) {
          manager = R2MultiPairWatermarkManager.decode(taskId, is);
        } else {
          manager = R2SinglePairWatermarkManager.decode(taskId, is);
        }

        final boolean isReceiveFinal = is.readBoolean();
        allPathStopped  = is.readBoolean();
        lambdaPathStopped = is.readBoolean();
        vmPathStopped = is.readBoolean();

        final boolean isLambdaRouter = is.readBoolean();

        LOG.info("Restore merger task {}: all path stoopped {}/ lambda path stooped {}/ vm path stopped {}" +
            "receive final /{}", taskId,
          allPathStopped, lambdaPathStopped, vmPathStopped, receiveFinal);

        receiveFinal = isReceiveFinal;

        if (receiveFinal) {
          dataHandler = new BypassDataHandler();
        } else {
          dataHandler = new ToMergerDataHandler();
        }

        if (isLambdaRouter) {
          dataRouter = new LambdaDataRouter(transientPathDstTask);
        } else {
          dataRouter = new VMDataRouter(vmPathDstTask);
        }

        LOG.info("Restore merger {}, toLambda: {}, receiveFinal: {}, dataRouter: {}",
          taskId, isLambdaRouter, receiveFinal, dataRouter);

        return manager;
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    } else {
      if (task.getTaskIncomingEdges().size() > 2) {
        return new R2MultiPairWatermarkManager(taskId);
      } else {
        return new R2SinglePairWatermarkManager(taskId);
      }
    }
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
    return mergerTransform.getNumKeys();
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

  private boolean allPathStopped = true;
  private boolean lambdaPathStopped = true;
  private boolean vmPathStopped = false;

  @Override
  public void stopInputPipeIndex(final Triple<String, String, String> triple) {
    final int taskIndex = RuntimeIdManager.getIndexFromTaskId(triple.getLeft());
    final String edgeId = triple.getMiddle();
    // taskWatermarkManager.stopIndex(taskIndex, edgeId);
    if (edgeId.equals(transientInputPathEdge.getId())) {
      lambdaPathStopped = taskWatermarkManager.stopIndex(taskIndex, edgeId);
    } else {
      vmPathStopped = taskWatermarkManager.stopIndex(taskIndex, edgeId);
    }

    allPathStopped = lambdaPathStopped || vmPathStopped;
    LOG.info("Stop input pipe index in {}, lambdaStop: {}, vmStop: {}, allStop: {} , {}",
      taskId, lambdaPathStopped, vmPathStopped, allPathStopped, triple);
  }

  @Override
  public void startInputPipeIndex(final Triple<String, String, String> triple) {
    try {
      final int taskIndex = RuntimeIdManager.getIndexFromTaskId(triple.getLeft());
      final String edgeId = triple.getMiddle();

      if (edgeId.equals(transientInputPathEdge.getId())) {
        lambdaPathStopped = false;
      } else {
        vmPathStopped = false;
      }
      allPathStopped = lambdaPathStopped || vmPathStopped;

      LOG.info("Start input pipe index in {}, lambdaStop: {}, vmStop: {}, allStop: {} , {}",
        taskId, lambdaPathStopped, vmPathStopped, allPathStopped, triple);

      taskWatermarkManager.startIndex(taskIndex, edgeId);
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Exception in " + taskId +
        ", vmEdgEId: " + vmInputPathEdge.getId()
      + ", lambdaEdgeId: " + transientInputPathEdge.getId()
      + " triple: " + triple);
    }
  }

  // R2
  @Override
  public void startAndStopInputPipeIndex(final Triple<String, String, String> triple) {
    LOG.info("Start and stop input pipe index {}", triple);
    final int taskIndex = RuntimeIdManager.getIndexFromTaskId(triple.getLeft());
    final String edgeId = triple.getMiddle();
    taskWatermarkManager.startAndStopPairIndex(taskIndex, edgeId);
  }

  @Override
  public void receivePartialFinal(final boolean finalResult) {
    if (!allPathStopped && finalResult) {
      throw new RuntimeException("All path not stopped, but merger receices final result " + taskId);
    }

    if (allPathStopped && !finalResult) {
      throw new RuntimeException("All path stopped, but merger receices partial result " + taskId);
    }

    LOG.info("Receive set partial/final {} in {}", finalResult, taskId);

    receiveFinal = finalResult;
    if (receiveFinal) {
      dataHandler = new BypassDataHandler();
    } else {
      dataHandler = new ToMergerDataHandler();
    }
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

      if (childVertex.isGBK) {
        isStateless = false;
        if (childVertex instanceof OperatorVertex) {
          final OperatorVertex ov = (OperatorVertex) childVertex;
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

        // Create VERTEX HARNESS
        final Transform.Context context = new TransformContextImpl(
          irVertex, serverlessExecutorProvider, taskId, stateStore,
          null,
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
                outputCollector, taskId);

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
    if (pairEdgeId.equals(transientOutputPathEdge.getId())) {
      // rerouting from VM to Lambda
      dataRouter = new LambdaDataRouter(transientPathDstTask);
    } else {
      // rerouting from Lambda to VM
      dataRouter = new VMDataRouter(vmPathDstTask);
    }
  }


  /*****************************************************************
   ************************ WATERMARK HANDLING *********************
   *****************************************************************
   * 현재 문제점:
   *  - TaskWatermarkHandler는 static하게 edge를 받아서, 모든 edge로부터 watermark를 기다림.
   *  - 만약 transient path로 data가 안가고 있다면, 이쪽으로 watermark도 보내지지 않을 것.
   *  - 따라서, data가 안가는 incoming edge를 판단해서 (stopped edge), 이 edge로는 watermark tracking X
   *    - a) input stop 시키고, state가 0이될 때까지 output은 stop시키지 않음.
   *    - b) state가 0이 되면, downstream task로 output stop signal 보냄.
   *    - c) taskWatermarkManager에서 input pipe stop되면 tracking하지 않음.
   */

  @Override
  public void handleData(final String edgeId,
                         final TaskHandlingEvent taskHandlingEvent) {
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
      throw new RuntimeException("Exception while hanlding data in " + taskId + ", from " + edgeId + ", data " + taskHandlingEvent.getClass());
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

  @Override
  public void restore() {

    throw new RuntimeException("SMTask not support restore " + taskId);
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
    final long st = System.currentTimeMillis();
    boolean hasChekpoint = false;

    try (final DataOutputStream os = new DataOutputStream(
      stateStore.getOutputStream(checkpointId + "-taskWatermarkManager"))) {

      if (task.getTaskIncomingEdges().size() > 2) {
        ((R2MultiPairWatermarkManager) taskWatermarkManager).encode(os);
      } else {
        ((R2SinglePairWatermarkManager) taskWatermarkManager).encode(taskId, os);
      }

      os.writeBoolean(receiveFinal);
      os.writeBoolean(allPathStopped);
      os.writeBoolean(lambdaPathStopped);
      os.writeBoolean(vmPathStopped);

      if (dataRouter instanceof LambdaDataRouter) {
        os.writeBoolean(true);
      } else {
        os.writeBoolean(false);
      }

      LOG.info("Checkpoint merger {}, toLambda: {}, receiveFinal: {}",
        taskId, dataRouter instanceof LambdaDataRouter, receiveFinal);

    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    mergerTransform.checkpoint(checkpointId);

    final long et = System.currentTimeMillis();
    LOG.info("Checkpoint elapsed time of {}: {}", taskId, et - st);

    return true;
  }

  @Override
  public String toString() {
    return taskId;
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

  final class BypassDataHandler implements DataHandler {
    @Override
    public void handleLocalData(Object data,
                                TaskHandlingEvent event) {
      if (data instanceof WatermarkWithIndex) {
        final WatermarkWithIndex watermarkWithIndex = (WatermarkWithIndex) data;
        // LOG.info("SM vertex receive watermark {} in {} from {}/{}", watermarkWithIndex.getWatermark(), taskId,
        //  watermarkWithIndex.getIndex(), event.getEdgeId());
        taskWatermarkManager.updateWatermark(event.getEdgeId(), watermarkWithIndex.getIndex(),
          watermarkWithIndex.getWatermark().getTimestamp())
          .ifPresent(watermark -> {
            taskMetrics.setInputWatermark(watermark.getTimestamp());
            if (getNumKeys() > 0) {
              mergerTransform.onWatermark(watermark);
            } else {
              dataRouter.writeData(new WatermarkWithIndex(watermark, taskIndex));
              dataHandler = new BypassOptimizeDataHandler();
            }
          });
      } else {
        taskMetrics.incrementInputProcessElement();
        dataRouter.writeData(data);
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
        // LOG.info("SM vertex receive watermark {} in {} from {}/{}", watermarkWithIndex.getWatermark(), taskId,
        //  watermarkWithIndex.getIndex(), taskHandlingEvent.getEdgeId());
        taskWatermarkManager.updateWatermark(taskHandlingEvent.getEdgeId(), watermarkWithIndex.getIndex(),
          watermarkWithIndex.getWatermark().getTimestamp())
          .ifPresent(watermark -> {
            taskMetrics.setInputWatermark(watermark.getTimestamp());
            if (getNumKeys() > 0) {
              mergerTransform.onWatermark(watermark);
            } else {
              dataRouter.writeData(new WatermarkWithIndex(watermark, taskIndex));
              dataHandler = new BypassOptimizeDataHandler();
            }
          });
      } else {
        // data
        taskMetrics.incrementInputProcessElement();
        dataRouter.writeByteBuf(data);
      }
    }
  }

  final class BypassOptimizeDataHandler implements DataHandler {
    @Override
    public void handleLocalData(Object data,
                                TaskHandlingEvent event) {
      if (data instanceof WatermarkWithIndex) {
        final WatermarkWithIndex watermarkWithIndex = (WatermarkWithIndex) data;
        // LOG.info("SM vertex receive watermark {} in {} from {}/{}", watermarkWithIndex.getWatermark(), taskId,
        //  watermarkWithIndex.getIndex(), event.getEdgeId());

        taskWatermarkManager.updateWatermark(event.getEdgeId(), watermarkWithIndex.getIndex(),
          watermarkWithIndex.getWatermark().getTimestamp())
          .ifPresent(watermark -> {
            taskMetrics.setInputWatermark(watermark.getTimestamp());
            dataRouter.writeData(new WatermarkWithIndex(watermark, taskIndex));
          });
      } else {

        // final long start = System.nanoTime();
        taskMetrics.incrementInputProcessElement();
        dataRouter.writeData(data);
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
        // LOG.info("SM vertex receive watermark {} in {} from {}/{}", watermarkWithIndex.getWatermark(), taskId,
        //  watermarkWithIndex.getIndex(), taskHandlingEvent.getEdgeId());

        taskWatermarkManager.updateWatermark(taskHandlingEvent.getEdgeId(), watermarkWithIndex.getIndex(),
          watermarkWithIndex.getWatermark().getTimestamp())
          .ifPresent(watermark -> {
            taskMetrics.setInputWatermark(watermark.getTimestamp());
            dataRouter.writeData(new WatermarkWithIndex(watermark, taskIndex));
          });
      } else {
        // data
        // final long start = System.nanoTime();
        taskMetrics.incrementInputProcessElement();
        dataRouter.writeByteBuf(data);
        // final long et = System.nanoTime();
        // taskMetrics.incrementComputation(et - start);
      }
    }
  }

  final class ToMergerDataHandler implements DataHandler {

    @Override
    public void handleLocalData(Object data, TaskHandlingEvent taskHandlingEvent) {
      if (data instanceof WatermarkWithIndex) {
        final WatermarkWithIndex watermarkWithIndex = (WatermarkWithIndex) data;
        // LOG.info("SM vertex receive watermark {} in {} from {}/{}", watermarkWithIndex.getWatermark(), taskId,
        //  watermarkWithIndex.getIndex(), taskHandlingEvent.getEdgeId());

        taskWatermarkManager.updateWatermark(taskHandlingEvent.getEdgeId(), watermarkWithIndex.getIndex(),
          watermarkWithIndex.getWatermark().getTimestamp())
          .ifPresent(watermark -> {
            taskMetrics.setInputWatermark(watermark.getTimestamp());
            // LOG.info("Emit SM watermark to merger in {} {}", taskId, watermark.getTimestamp());
            mergerTransform.onWatermark(watermark);
          });
      } else {
        // LOG.info("Emit SM data to merger in {} {}", taskId);
        final TimestampAndValue event = (TimestampAndValue) taskHandlingEvent.getData();
        // final long ns = System.nanoTime();
        taskMetrics.incrementInputProcessElement();

        outputCollector.setInputTimestamp(event.timestamp);
        mergerTransform.onData(event.value);

        // final long endNs = System.nanoTime();
        // taskMetrics.incrementComputation(endNs - ns);
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
        // LOG.info("SM vertex receive watermark {} in {} from {}/{}", watermarkWithIndex.getWatermark(), taskId,
        //  watermarkWithIndex.getIndex(), taskHandlingEvent.getEdgeId());

        taskWatermarkManager.updateWatermark(taskHandlingEvent.getEdgeId(), watermarkWithIndex.getIndex(),
          watermarkWithIndex.getWatermark().getTimestamp())
          .ifPresent(watermark -> {
            taskMetrics.setInputWatermark(watermark.getTimestamp());
            // LOG.info("Emit SM watermark to merger in {} {}", taskId, watermark.getTimestamp());
            mergerTransform.onWatermark(watermark);
          });
      } else {
        // data
        // LOG.info("Emit SM data to merger in {} {}", taskId);
        final TimestampAndValue event = (TimestampAndValue) taskHandlingEvent.getData();
        // final long ns = System.nanoTime();
        taskMetrics.incrementInputProcessElement();

        outputCollector.setInputTimestamp(event.timestamp);
        mergerTransform.onData(event.value);

        // final long endNs = System.nanoTime();
        // taskMetrics.incrementComputation(endNs - ns);
      }
    }
  }

  interface DataRouter {
    void writeData(Object data);
    void writeByteBuf(ByteBuf data);
  }

  final class LambdaDataRouter implements DataRouter {
    private final String lambdaTaskId;

    public LambdaDataRouter(final String lambdaTaskId) {
      this.lambdaTaskId = lambdaTaskId;
    }

    @Override
    public void writeData(Object data) {
      pipeManagerWorker.writeData(taskId, transientOutputPathEdge.getId(),
        lambdaTaskId,
        transientPathSerializer,
        data);
    }

    @Override
    public void writeByteBuf(ByteBuf data) {
      pipeManagerWorker.writeByteBufData(taskId,
        transientOutputPathEdge.getId(), lambdaTaskId, data);
    }
  }

  final class VMDataRouter implements DataRouter {
    private final String vmTaskId;

    public VMDataRouter(final String vmTaskId) {
      this.vmTaskId = vmTaskId;
    }

    @Override
    public void writeData(Object data) {
      pipeManagerWorker.writeData(taskId, vmOutputPathEdge.getId(),
        vmTaskId,
        vmPathSerializer,
        data);
    }

    @Override
    public void writeByteBuf(ByteBuf data) {
      pipeManagerWorker.writeByteBufData(taskId, vmOutputPathEdge.getId(),
        vmTaskId, data);
    }
  }

  interface GetDstTaskId {
    int getDstTaskIdIndex();
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
