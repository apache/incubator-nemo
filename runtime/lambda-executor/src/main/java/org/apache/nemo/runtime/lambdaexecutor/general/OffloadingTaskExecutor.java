package org.apache.nemo.runtime.lambdaexecutor.general;

import avro.shaded.com.google.common.collect.Lists;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.nemo.common.*;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.Edge;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.punctuation.EmptyElement;
import org.apache.nemo.common.punctuation.TimestampAndValue;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.compiler.frontend.beam.source.UnboundedSourceReadable;
import org.apache.nemo.compiler.frontend.beam.transform.GBKFinalState;
import org.apache.nemo.compiler.frontend.beam.transform.GBKFinalTransform;
import org.apache.nemo.compiler.frontend.beam.transform.StatefulTransform;
import org.apache.nemo.offloading.common.OffloadingOutputCollector;
import org.apache.nemo.offloading.common.StateStore;
import org.apache.nemo.runtime.executor.common.*;
import org.apache.nemo.runtime.executor.common.datatransfer.*;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingResultCollector;
import org.apache.nemo.runtime.lambdaexecutor.ReadyTask;
import org.apache.nemo.runtime.lambdaexecutor.StateOutput;
import org.apache.nemo.runtime.lambdaexecutor.datatransfer.*;
import org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOffloadingOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public final class OffloadingTaskExecutor implements TaskExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(OffloadingTaskExecutor.class.getName());

  private final OffloadingTask offloadingTask;
  private ReadyTask readyTask;
  private final OffloadingOutputCollector oc;
  private final OffloadingResultCollector resultCollector;

  private final IntermediateDataIOFactory intermediateDataIOFactory;
  // private final Map<String, KafkaOperatorVertexOutputCollector> outputCollectorMap;
  private final Map<String, NextIntraTaskOperatorInfo> operatorVertexMap;

  private final Map<String, Serializer> serializerMap;
 // private final Set<OffloadingPipeOutputWriter> pipeOutputWriters;

  private final List<SourceVertexDataFetcher> sourceVertexDataFetchers;

  private boolean isStateless = true;

  private GBKFinalTransform gbkFinalTransform;

  private boolean finished = false;

  private final List<Future<Integer>> pendingFutures = new ArrayList<>();

  private final ExecutorService prepareService;

  private final ExecutorGlobalInstances executorGlobalInstances;
  final List<DataFetcher> allFetchers = new ArrayList<>();

  private final RendevousServerClient rendevousServerClient;

  private Transform statefulTransform;

  private final ExecutorThread executorThread;

  // TODO: we should get checkpoint mark in constructor!

  final AtomicBoolean prepared = new AtomicBoolean(false);

  private final List<Future> outputfutures = new ArrayList<>();

  final Map<String, Double> samplingMap;

  final TaskMetrics taskMetrics;

  private final Map<String, NextIntraTaskOperatorInfo> operatorInfoMap = new HashMap<>();

  private final String taskId;

  private final OutputCollectorGenerator outputCollectorGenerator;

  private final SerializerManager serializerManager;

  private final Map<String, Pair<OperatorMetricCollector, OutputCollector>> vertexIdAndCollectorMap;

  private final StateStore stateStore;

  private final Map<String, DataFetcher> edgeToDataFetcherMap = new HashMap<>();

  private final ExecutorThreadQueue executorThreadQueue;

  private final InputPipeRegister inputPipeRegister;

  // TODO: Data fetcher watermark manager
  public OffloadingTaskExecutor(final OffloadingTask offloadingTask,
                                final Map<String, Serializer> serializerMap,
                                final IntermediateDataIOFactory intermediateDataIOFactory,
                                final OffloadingOutputCollector oc,
                                final ExecutorService prepareService,
                                final ExecutorGlobalInstances executorGlobalInstances,
                                final RendevousServerClient rendevousServerClient,
                                final ExecutorThread executorThread,
                                final Map<String, Double> samplingMap,
                                final Task task,
                                final OutputCollectorGenerator outputCollectorGenerator,
                                final SerializerManager serializerManager,
                                final StateStore stateStore,
                                final ExecutorThreadQueue executorThreadQueue,
                                final InputPipeRegister inputPipeRegister) {
    this.offloadingTask = offloadingTask;
    this.inputPipeRegister = inputPipeRegister;
    this.serializerMap = serializerMap;
    this.intermediateDataIOFactory = intermediateDataIOFactory;
    this.oc = oc;
    this.sourceVertexDataFetchers = new ArrayList<>();
    // this.outputCollectorMap = new HashMap<>();
    this.operatorVertexMap = new HashMap<>();
  //  this.pipeOutputWriters = new HashSet<>();
    this.resultCollector = new OffloadingResultCollector(oc);
    this.prepareService = prepareService;
    this.executorGlobalInstances = executorGlobalInstances;
    this.rendevousServerClient = rendevousServerClient;
    this.executorThread = executorThread;
    this.taskMetrics = new TaskMetrics();
    this.samplingMap = samplingMap;
    this.taskId = task.getTaskId();
    this.outputCollectorGenerator = outputCollectorGenerator;
    this.serializerManager = serializerManager;
    this.vertexIdAndCollectorMap = new HashMap<>();
    this.stateStore = stateStore;
    this.executorThreadQueue = executorThreadQueue;

    /*
    pollingTrigger.scheduleAtFixedRate(() -> {
      pollingTime = true;
    }, pollingInterval, pollingInterval, TimeUnit.MILLISECONDS);
    */

    prepare(task);
  }

  private RuntimeEdge<IRVertex> getEdge(final DAG<IRVertex, RuntimeEdge<IRVertex>> dag,
                                        final IRVertex vertex) {
    if (dag.getOutgoingEdgesOf(vertex).size() == 0) {
      return dag.getIncomingEdgesOf(vertex).get(0);
    } else {
      return dag.getOutgoingEdgesOf(vertex).get(0);
    }
  }

  public synchronized void start(final ReadyTask readyTask) {
    this.readyTask = readyTask;


    final List<IRVertex> reverseTopologicallySorted = Lists.reverse(offloadingTask.irDag.getTopologicalSort());


    // pipe output writer prepare
//    LOG.info("Pipe output writers: {}", pipeOutputWriters.size());
//    pipeOutputWriters.forEach(pipeOutputWriter -> {
//      pipeOutputWriter.doInitialize();
//    });

    reverseTopologicallySorted.forEach(irVertex -> {
      final Transform transform;
      if (irVertex instanceof OperatorVertex) {
        transform = ((OperatorVertex) irVertex).getTransform();
        if (transform instanceof StatefulTransform) {
          isStateless = false;

          final GBKFinalState state = readyTask.stateMap.get(irVertex.getId());
          if (state != null) {
            LOG.info("Set state for operator {}", irVertex.getId());
            final StatefulTransform statefulTransform = (StatefulTransform) transform;
            statefulTransform.setState(state);

            if (statefulTransform instanceof GBKFinalTransform) {
              gbkFinalTransform = (GBKFinalTransform) statefulTransform;
            }
          }
        }

//        final OutputCollector outputCollector = outputCollectorMap.get(irVertex.getId());
//        transform.prepare(new OffloadingTransformContextImpl(irVertex, offloadingTask.taskId), outputCollector);
      }
    });

    for (final DataFetcher dataFetcher : allFetchers) {

      if (dataFetcher instanceof SourceVertexDataFetcher) {
        final UnboundedSource.CheckpointMark checkpointMark = readyTask.checkpointMark;
        final UnboundedSource unboundedSource = readyTask.unboundedSource;
        LOG.info("Receive checkpointmark: {}", checkpointMark);
        final UnboundedSourceReadable readable =
          new UnboundedSourceReadable(unboundedSource, null, checkpointMark);

        final SourceVertexDataFetcher sourceVertexDataFetcher = (SourceVertexDataFetcher) dataFetcher;
        // sourceVertexDataFetcher.setReadable(readable);

      } else if (dataFetcher instanceof LambdaParentTaskDataFetcher) {

        final LambdaParentTaskDataFetcher lambdaParentTaskDataFetcher = (LambdaParentTaskDataFetcher) dataFetcher;
        lambdaParentTaskDataFetcher.prepare();
      }
    }

    prepared.set(true);
  }

  private synchronized void prepare(final Task task) {
    final int taskIndex = RuntimeIdManager.getIndexFromTaskId(task.getTaskId());

    System.out.println("OffloadingTaskExecutor prepare");
    final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag = offloadingTask.irDag;
    // Traverse in a reverse-topological order to ensure that each visited vertex's children vertices exist.
    final List<IRVertex> reverseTopologicallySorted = Lists.reverse(irVertexDag.getTopologicalSort());

    // Build a map for edge as a key and edge index as a value
    // This variable is used for creating NextIntraTaskOperatorInfo
    // in {@link this#getInternalMainOutputs and this#internalMainOutputs}
    reverseTopologicallySorted.forEach(childVertex -> {

      if (childVertex.isStateful) {
        isStateless = false;
        if (childVertex instanceof OperatorVertex) {
          final OperatorVertex ov = (OperatorVertex) childVertex;
          statefulTransform = ov.getTransform();
          LOG.info("Set GBK final transform");
        }
      }

      if (irVertexDag.getOutgoingEdgesOf(childVertex.getId()).size() == 0) {
        childVertex.isSink = true;
        // If it is sink or emit to next stage, we log the latency
        LOG.info("MonitoringVertex: {}", childVertex.getId());
        if (!samplingMap.containsKey(childVertex.getId())) {
          samplingMap.put(childVertex.getId(), 1.0);
        }
        LOG.info("Sink vertex: {}", childVertex.getId());
      }
    });

    // Create a harness for each vertex
    reverseTopologicallySorted.forEach(irVertex -> {
      final Optional<Readable> sourceReader = TaskExecutorUtil
        .getSourceVertexReader(irVertex, task.getIrVertexIdToReadable());

      if (sourceReader.isPresent() != irVertex instanceof SourceVertex) {
        throw new IllegalStateException(irVertex.toString());
      }

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
          operatorInfoMap);

      // Create VERTEX HARNESS
      final Transform.Context context =  new TransformContextImpl(
        irVertex, null, taskId, stateStore);

      TaskExecutorUtil.prepareTransform(irVertex, context, outputCollector);

      // Prepare data READ
      // Source read
      // TODO[SLS]: should consider multiple outgoing edges
      // All edges will have the same encoder/decoder!
      if (irVertex instanceof SourceVertex) {
        final RuntimeEdge edge = irVertexDag.getOutgoingEdgesOf(irVertex).get(0);
        LOG.info("SourceVertex: {}, edge: {}", irVertex.getId(), edge.getId());

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
          });

        edgeToDataFetcherMap.put(edge.getId(), fe);

        sourceVertexDataFetchers.add(fe);
        allFetchers.add(fe);

        if (sourceVertexDataFetchers.size() > 1) {
          throw new RuntimeException("Source vertex data fetcher is larger than one");
        }
      }

      // Parent-task read
      // TODO #285: Cache broadcasted data
      task.getTaskIncomingEdges()
        .stream()
        .filter(inEdge -> inEdge.getDstIRVertex().getId().equals(irVertex.getId())) // edge to this vertex
        .map(incomingEdge -> {

          LOG.info("Incoming edge: {}, taskIndex: {}, taskId: {}", incomingEdge, taskIndex, taskId);

          return Pair.of(incomingEdge, intermediateDataIOFactory
            .createReader(
              taskIndex,
              taskId,
              incomingEdge.getSrcIRVertex(), incomingEdge, executorThreadQueue));
        })
        .forEach(pair -> {
          if (irVertex instanceof OperatorVertex) {

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

            if (comm.equals(CommunicationPatternProperty.Value.OneToOne)) {
              inputPipeRegister.registerInputPipe(
                RuntimeIdManager.generateTaskId(edge.getSrc().getId(), taskIndex, 0),
                edge.getId(),
                task.getTaskId(),
                parentTaskReader);

//              taskWatermarkManager.addDataFetcher(df.getEdgeId(), 1);

            } else {
              for (int i = 0; i < parallelism; i++) {
                inputPipeRegister.registerInputPipe(
                  RuntimeIdManager.generateTaskId(edge.getSrc().getId(), i, 0),
                  edge.getId(),
                  task.getTaskId(),
                  parentTaskReader);
              }

//              taskWatermarkManager.addDataFetcher(df.getEdgeId(), parallelism);
            }

            isStateless = false;
            allFetchers.add(df);
          }
        });
    });

  }

  private void processWatermark(final OutputCollector outputCollector,
                                final Watermark watermark) {
    outputCollector.emitWatermark(watermark);
  }

  /**
   * Process a data element down the DAG dependency.
   */
  private void processElement(final OutputCollector outputCollector, final TimestampAndValue dataElement) {

    //LOG.info("Process element {}", dataElement.value);
    final long ns = System.nanoTime();

    outputCollector.setInputTimestamp(dataElement.timestamp);
    outputCollector.emit(dataElement.value);

    final long endNs = System.nanoTime();

    final long comp = (long) ((endNs - ns) /
      ScalingPolicyParameters.LAMBDA_CPU_PROC_TIME_RATIO); // 2: 보정값.

    taskMetrics.incrementComputation(comp);
  }

  private void onEventFromDataFetcher(final Object event,
                                      final DataFetcher dataFetcher) {

    if (event instanceof Watermark) {
      // Watermark
      processWatermark(dataFetcher.getOutputCollector(), (Watermark) event);
    } else if (event instanceof TimestampAndValue) {
      // Process data element
      taskMetrics.incrementInputElement();
      processElement(dataFetcher.getOutputCollector(), (TimestampAndValue) event);

    } else {
      throw new RuntimeException("Invalid type of event: " + event);
    }
  }

  public void close() {
    for (final DataFetcher dataFetcher : allFetchers) {
      LOG.info("Stopping data fetcher of {}/ {}", offloadingTask.taskId, dataFetcher);
      pendingFutures.add(dataFetcher.stop(offloadingTask.taskId));
    }

    LOG.info("Waiting pending futures haha {}...", offloadingTask.taskId);
    finished = true;
  }

  private Pair<Map<String, GBKFinalState>, Map<String, Coder<GBKFinalState>>> getStateAndCoderMap() {
    final Map<String, GBKFinalState> stateMap = new HashMap<>();
    final Map<String, Coder<GBKFinalState>> coderMap = new HashMap<>();
    for (final IRVertex vertex : offloadingTask.irDag.getVertices()) {
      if (vertex instanceof OperatorVertex) {
        final Transform transform = ((OperatorVertex) vertex).getTransform();
        if (transform instanceof StatefulTransform) {
          final StatefulTransform finalTransform = (StatefulTransform) transform;
          stateMap.put(vertex.getId(), (GBKFinalState) finalTransform.getState());
          coderMap.put(vertex.getId(), finalTransform.getStateCoder());
        }
      }
    }

    return Pair.of(stateMap, coderMap);
  }

  @Override
  public boolean checkpoint() {

    try {
      for (final Future future : outputfutures) {
        if (!future.isDone()) {
          return false;
        } else {
          future.get();
        }
      }

      outputfutures.clear();

      LOG.info("All Clossed output writer {}", offloadingTask.taskId);

      //Thread.sleep(3000);

      // TODO: we send checkpoint mark to vm
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    boolean hasSource = false;
    for (final DataFetcher dataFetcher : allFetchers) {

      if (dataFetcher instanceof SourceVertexDataFetcher) {
        hasSource = true;
        // send checkpoint mark to the VM!!
        final Pair<Map<String, GBKFinalState>, Map<String, Coder<GBKFinalState>>>
          stateAndCoderMap = getStateAndCoderMap();

        final Map<String, GBKFinalState> stateMap = stateAndCoderMap.left();

        final SourceVertexDataFetcher srcDataFetcher = (SourceVertexDataFetcher) dataFetcher;
        if (srcDataFetcher.isStarted()) {
          final UnboundedSourceReadable readable = (UnboundedSourceReadable) srcDataFetcher.getReadable();
          final UnboundedSource.CheckpointMark checkpointMark = readable.getReader().getCheckpointMark();
          final Coder<UnboundedSource.CheckpointMark> checkpointMarkCoder = readable.getUnboundedSource().getCheckpointMarkCoder();


            LOG.info("Send checkpointmark of task {} move {}/ {}", offloadingTask.taskId,
              deleteForMove,
              checkpointMark);
            resultCollector.collector.emit(new KafkaOffloadingOutput(deleteForMove,
              offloadingTask.taskId, 1, checkpointMark,
              checkpointMarkCoder, stateMap, stateAndCoderMap.right()));

        } else {
          final UnboundedSourceReadable readable = (UnboundedSourceReadable) srcDataFetcher.getReadable();
          final Coder<UnboundedSource.CheckpointMark> checkpointMarkCoder = readable.getUnboundedSource().getCheckpointMarkCoder();

          LOG.info("Send checkpointmark of task {} move {} / {} to vm",
            offloadingTask.taskId,  deleteForMove, readyTask.checkpointMark);
          resultCollector.collector.emit(new KafkaOffloadingOutput(deleteForMove,
            offloadingTask.taskId, 1, readyTask.checkpointMark, checkpointMarkCoder, stateMap, stateAndCoderMap.right()));
        }
      }
    }

    if (!hasSource) {
      // send states to vm !!
      LOG.info("Send  stateoutput for task {}", offloadingTask.taskId);
      final Pair<Map<String, GBKFinalState>, Map<String, Coder<GBKFinalState>>>
        stateAndCoderMap = getStateAndCoderMap();

      final Map<String, GBKFinalState> stateMap = stateAndCoderMap.left();
      resultCollector.collector.emit(new StateOutput(
        deleteForMove,
        offloadingTask.taskId, stateMap, stateAndCoderMap.right()));
    }

    pendingFutures.clear();

    return true;
  }

  public void finish() {

    LOG.info("Finishing {}", offloadingTask.taskId);

    /*
    while (!executorThread.queue.isEmpty()) {
      LOG.info("Waiting for executor finish, numEvent: {}", executorThread.queue);
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    */

    // TODO: fix
//    outputfutures.addAll(pipeOutputWriters.stream()
//      .map(outputWriter -> {
//        return outputWriter.close(offloadingTask.taskId);
//      }).collect(Collectors.toList()));

    LOG.info("Closing output writer {}", offloadingTask.taskId);
  }

  // Get all of the intra-task edges
  private static List<Edge> getAllIncomingEdges(
    final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
    final IRVertex childVertex,
    final List<StageEdge> incomingEdges) {
    final List<Edge> edges = new ArrayList<>();
    edges.addAll(irVertexDag.getIncomingEdgesOf(childVertex));
    final List<StageEdge> taskEdges = incomingEdges.stream()
      .filter(edge -> edge.getDstIRVertex().getId().equals(childVertex.getId()))
      .collect(Collectors.toList());
    edges.addAll(taskEdges);
    return edges;
  }

  private boolean allPendingDone() {
    for (final Future<Integer> pendingFuture : pendingFutures) {
      if (!pendingFuture.isDone()) {
        return false;
      }
    }
    return true;
  }

  public boolean isInputFinished() {
    return finished && allPendingDone() && executorThread.isEmpty();
  }



  @Override
  public AtomicLong getTaskExecutionTime() {
    return null;
  }

  @Override
  public long getThreadId() {
    return 0;
  }

  @Override
  public String getId() {
    return offloadingTask.taskId;
  }

  @Override
  public boolean isStateless() {
    return false;
  }

  @Override
  public AtomicInteger getProcessedCnt() {
    throw new UnsupportedOperationException();
  }

  // exeutor thread가 바로 부르는 method
  @Override
  public boolean handleSourceData() {
    boolean processed = false;

    for (final SourceVertexDataFetcher dataFetcher : sourceVertexDataFetchers) {
      final Object event = dataFetcher.fetchDataElement();
      if (!event.equals(EmptyElement.getInstance()))  {
        onEventFromDataFetcher(event, dataFetcher);
        processed = true;
      }
    }
    return processed;
  }

  @Override
  public boolean isSourceAvailable() {
    for (final SourceVertexDataFetcher sourceVertexDataFetcher : sourceVertexDataFetchers) {
      if (sourceVertexDataFetcher.isAvailable()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean hasData() {
        for (final SourceVertexDataFetcher sourceVertexDataFetcher : sourceVertexDataFetchers) {
      if (sourceVertexDataFetcher.hasData()) {
        return true;
      }
    }

    return false;
  }

  @Override
  public void handleData(String edgeId, TaskHandlingEvent t) {

  }

  private boolean deleteForMove = false;

  @Override
  public Task getTask() {
    return null;
  }

  @Override
  public int getNumKeys() {
    if (isStateless) {
      return 0;
    } else {
      //LOG.info("Key {}, {}", num, taskId);
      return gbkFinalTransform.getNumKeys();
    }
  }

  @Override
  public TaskMetrics getTaskMetrics() {
    return taskMetrics;
  }


  @Override
  public boolean isSource() {
    return sourceVertexDataFetchers.size() > 0;
  }


  /*
  @Override
  public void handleIntermediateData(IteratorWithNumBytes iterator, DataFetcher dataFetcher) {
    if (iterator.hasNext()) {
      executorThread.decoderThread.execute(() -> {
        while (iterator.hasNext()) {
          final Object element = iterator.next();
          if (prepared.get()) {
            executorThread.queue.add(() -> {
              if (!element.equals(EmptyElement.getInstance())) {
                //LOG.info("handle intermediate data {}, {}", element, dataFetcher);
                onEventFromDataFetcher(element, dataFetcher);
              }
            });
          }
        }
      });
    }
  }
  */

  @Override
  public void setIRVertexPutOnHold(IRVertex irVertex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString() {
    return getId();
  }
}
