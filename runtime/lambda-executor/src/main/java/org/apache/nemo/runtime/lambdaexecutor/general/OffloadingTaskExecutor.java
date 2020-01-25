package org.apache.nemo.runtime.lambdaexecutor.general;

import avro.shaded.com.google.common.collect.Lists;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ScalingPolicyParameters;
import org.apache.nemo.common.TaskMetrics;
import org.apache.nemo.common.Util;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.Edge;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.ir.edge.executionproperty.AdditionalOutputTagProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.punctuation.EmptyElement;
import org.apache.nemo.common.punctuation.Finishmark;
import org.apache.nemo.common.punctuation.TimestampAndValue;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.compiler.frontend.beam.source.BeamUnboundedSourceVertex;
import org.apache.nemo.compiler.frontend.beam.source.UnboundedSourceReadable;
import org.apache.nemo.compiler.frontend.beam.transform.GBKFinalState;
import org.apache.nemo.compiler.frontend.beam.transform.GBKFinalTransform;
import org.apache.nemo.compiler.frontend.beam.transform.StatefulTransform;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.offloading.common.OffloadingOutputCollector;
import org.apache.nemo.runtime.executor.common.*;
import org.apache.nemo.runtime.executor.common.datatransfer.DataFetcherOutputCollector;
import org.apache.nemo.runtime.executor.common.datatransfer.InputReader;
import org.apache.nemo.runtime.executor.common.datatransfer.IteratorWithNumBytes;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingResultCollector;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingTransformContextImpl;
import org.apache.nemo.runtime.lambdaexecutor.ReadyTask;
import org.apache.nemo.runtime.lambdaexecutor.StateOutput;
import org.apache.nemo.runtime.lambdaexecutor.datatransfer.*;
import org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOffloadingOutput;
import org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOperatorVertexOutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
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
  private final Map<String, KafkaOperatorVertexOutputCollector> outputCollectorMap;
  private final Map<String, NextIntraTaskOperatorInfo> operatorVertexMap;

  private final Map<String, Serializer> serializerMap;
  private final Set<PipeOutputWriter> pipeOutputWriters;

  private final List<SourceVertexDataFetcher> sourceVertexDataFetchers;

  private boolean isStateless = true;

  private GBKFinalTransform gbkFinalTransform;

  private boolean finished = false;

  private final List<Future<Integer>> pendingFutures = new ArrayList<>();

  private final ExecutorService prepareService;

  private final ExecutorGlobalInstances executorGlobalInstances;
  final List<DataFetcher> allFetchers = new ArrayList<>();

  private final RendevousServerClient rendevousServerClient;

  private final ExecutorThread executorThread;

  // TODO: we should get checkpoint mark in constructor!

  final AtomicBoolean prepared = new AtomicBoolean(false);

  private final List<Future> outputfutures = new ArrayList<>();

  final TaskMetrics taskMetrics;

  public OffloadingTaskExecutor(final OffloadingTask offloadingTask,
                                final Map<String, Serializer> serializerMap,
                                final IntermediateDataIOFactory intermediateDataIOFactory,
                                final OffloadingOutputCollector oc,
                                final ExecutorService prepareService,
                                final ExecutorGlobalInstances executorGlobalInstances,
                                final RendevousServerClient rendevousServerClient,
                                final ExecutorThread executorThread) {
    this.offloadingTask = offloadingTask;
    this.serializerMap = serializerMap;
    this.intermediateDataIOFactory = intermediateDataIOFactory;
    this.oc = oc;
    this.sourceVertexDataFetchers = new ArrayList<>();
    this.outputCollectorMap = new HashMap<>();
    this.operatorVertexMap = new HashMap<>();
    this.pipeOutputWriters = new HashSet<>();
    this.resultCollector = new OffloadingResultCollector(oc);
    this.prepareService = prepareService;
    this.executorGlobalInstances = executorGlobalInstances;
    this.rendevousServerClient = rendevousServerClient;
    this.executorThread = executorThread;
    this.taskMetrics = new TaskMetrics();

    /*
    pollingTrigger.scheduleAtFixedRate(() -> {
      pollingTime = true;
    }, pollingInterval, pollingInterval, TimeUnit.MILLISECONDS);
    */

    prepare();
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
    LOG.info("Pipe output writers: {}", pipeOutputWriters.size());
    pipeOutputWriters.forEach(pipeOutputWriter -> {
      pipeOutputWriter.doInitialize();
    });

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

        final OutputCollector outputCollector = outputCollectorMap.get(irVertex.getId());
        transform.prepare(new OffloadingTransformContextImpl(irVertex, offloadingTask.taskId), outputCollector);
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
        sourceVertexDataFetcher.setReadable(readable);

      } else if (dataFetcher instanceof LambdaParentTaskDataFetcher) {

        final LambdaParentTaskDataFetcher lambdaParentTaskDataFetcher = (LambdaParentTaskDataFetcher) dataFetcher;
        lambdaParentTaskDataFetcher.prepare();
      }
    }

    prepared.set(true);
  }

  private synchronized void prepare() {

    System.out.println("Stateless offloading transform prepare");
    // Traverse in a reverse-topological order to ensure that each visited vertex's children vertices exist.
    final List<IRVertex> reverseTopologicallySorted = Lists.reverse(offloadingTask.irDag.getTopologicalSort());

    // Build a map for edge as a key and edge index as a value
    // This variable is used for creating NextIntraTaskOperatorInfo
    // in {@link this#getInternalMainOutputs and this#internalMainOutputs}
    final AtomicInteger sourceCnt = new AtomicInteger(0);
    final Map<Edge, Integer> edgeIndexMap = new HashMap<>();
    reverseTopologicallySorted.forEach(childVertex -> {

      if (childVertex instanceof BeamUnboundedSourceVertex) {
        sourceCnt.getAndIncrement();
      }

      final List<Edge> edges = getAllIncomingEdges(offloadingTask.irDag,
        childVertex, offloadingTask.incomingEdges);
      for (int edgeIndex = 0; edgeIndex < edges.size(); edgeIndex++) {
        final Edge edge = edges.get(edgeIndex);
        edgeIndexMap.putIfAbsent(edge, edgeIndex);
      }
    });

    // Build a map for InputWatermarkManager for each operator vertex
    // This variable is used for creating NextIntraTaskOperatorInfo
    // in {@link this#getInternalMainOutputs and this#internalMainOutputs}
    final Map<IRVertex, InputWatermarkManager> operatorWatermarkManagerMap = new HashMap<>();
    reverseTopologicallySorted.forEach(childVertex -> {

      if (childVertex instanceof OperatorVertex) {
        final List<Edge> edges = getAllIncomingEdges(offloadingTask.irDag,
          childVertex, offloadingTask.incomingEdges);
        if (edges.size() == 1) {
          operatorWatermarkManagerMap.putIfAbsent(childVertex,
            new SingleInputWatermarkManager(
              new OperatorWatermarkCollector((OperatorVertex) childVertex),
              null, null, null, null));
        } else {
          operatorWatermarkManagerMap.putIfAbsent(childVertex,
            new MultiInputWatermarkManager(null, edges.size(),
              new OperatorWatermarkCollector((OperatorVertex) childVertex),
              offloadingTask.taskId));
        }
      }
    });

    reverseTopologicallySorted.forEach(irVertex -> {

      // Additional outputs
      final Map<String, List<NextIntraTaskOperatorInfo>> internalAdditionalOutputMap =
        getInternalAdditionalOutputMap(irVertex, offloadingTask.irDag, edgeIndexMap, operatorWatermarkManagerMap);

      // Main outputs
      final List<NextIntraTaskOperatorInfo> internalMainOutputs =
        getInternalMainOutputs(irVertex, offloadingTask.irDag, edgeIndexMap, operatorWatermarkManagerMap);

      for (final List<NextIntraTaskOperatorInfo> interOps : internalAdditionalOutputMap.values()) {
        for (final NextIntraTaskOperatorInfo interOp : interOps) {
          operatorVertexMap.put(interOp.getNextOperator().getId(), interOp);
        }
      }

      final Map<String, List<PipeOutputWriter>> externalAdditionalOutputMap =
        getExternalAdditionalOutputMap(
          irVertex,
          offloadingTask.outgoingEdges,
          intermediateDataIOFactory,
          offloadingTask.taskIndex,
          offloadingTask.taskIndex,
          serializerMap, pipeOutputWriters,
          offloadingTask.taskId,
          rendevousServerClient,
          executorThread,
          taskMetrics);

      LOG.info("External additional outputs at {}: {}", offloadingTask.taskId, externalAdditionalOutputMap);

      final List<PipeOutputWriter> externalMainOutputs = getExternalMainOutputs(
        irVertex, offloadingTask.outgoingEdges,
        intermediateDataIOFactory,
        offloadingTask.taskIndex,
        offloadingTask.taskIndex, serializerMap, pipeOutputWriters,
        offloadingTask.taskId,
        rendevousServerClient,
        executorThread,
        taskMetrics);

      LOG.info("External main outputs at {}: {}", offloadingTask.taskId, externalMainOutputs);

      for (final NextIntraTaskOperatorInfo interOp : internalMainOutputs) {
        operatorVertexMap.put(interOp.getNextOperator().getId(), interOp);
      }

      final boolean isSink = irVertex.isSink;
      // skip sink
      System.out.println("vertex " + irVertex.getId() + " outgoing edges: " + offloadingTask.irDag.getOutgoingEdgesOf(irVertex)
        + ", isSink: " + isSink);

      final RuntimeEdge<IRVertex> e = getEdge(offloadingTask.irDag, irVertex);
      KafkaOperatorVertexOutputCollector outputCollector =
        new KafkaOperatorVertexOutputCollector(
          offloadingTask.taskId,
          irVertex,
          offloadingTask.samplingMap.getOrDefault(irVertex.getId(), 1.0),
          e, /* just use first edge for encoding */
          internalMainOutputs,
          internalAdditionalOutputMap,
          oc,
          outputCollectorMap,
          offloadingTask.taskOutgoingEdges,
          externalAdditionalOutputMap,
          externalMainOutputs);

      outputCollectorMap.put(irVertex.getId(), outputCollector);

      // TODO: fix
      // DATA FETCHERS!!
      // get source
      if (irVertex instanceof BeamUnboundedSourceVertex) {
        final BeamUnboundedSourceVertex beamUnboundedSourceVertex = (BeamUnboundedSourceVertex) irVertex;
        final RuntimeEdge edge = offloadingTask.irDag.getOutgoingEdgesOf(irVertex).get(0);

        final SourceVertexDataFetcher dataFetcher = new SourceVertexDataFetcher(
          beamUnboundedSourceVertex, edge, null /* readable */, outputCollector, prepareService, offloadingTask.taskId,
          executorGlobalInstances, prepared);
        allFetchers.add(dataFetcher);
        sourceVertexDataFetchers.add(dataFetcher);
      }

      // task incoming edges!!
      offloadingTask.incomingEdges
        .stream()
        .filter(inEdge -> inEdge.getDstIRVertex().getId().equals(irVertex.getId()))
        .map(incomingEdge -> {

          LOG.info("Incoming edge: {}, taskIndex: {}, taskId: {}", incomingEdge, offloadingTask.taskIndex,
            offloadingTask.taskId);

          return Pair.of(incomingEdge, intermediateDataIOFactory
            .createReader(offloadingTask.taskIndex, incomingEdge.getSrcIRVertex(), incomingEdge,
              offloadingTask.taskId, this));
        })
        .forEach(pair -> {
          if (irVertex instanceof OperatorVertex) {

            final StageEdge edge = pair.left();
            final int edgeIndex = edgeIndexMap.get(edge);
            final InputWatermarkManager watermarkManager = operatorWatermarkManagerMap.get(irVertex);
            final InputReader parentTaskReader = pair.right();
            final OutputCollector dataFetcherOutputCollector =
              new DataFetcherOutputCollector(edge.getSrcIRVertex(), (OperatorVertex) irVertex,
                outputCollector, edgeIndex, watermarkManager);
            allFetchers.add(
              new LambdaParentTaskDataFetcher(
                offloadingTask.taskId,
                parentTaskReader.getSrcIrVertex(),
                edge,
                parentTaskReader,
                dataFetcherOutputCollector,
                rendevousServerClient,
                this,
                prepared,
                prepareService));

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

  @Override
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
  public boolean isFinishDone() {

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

          LOG.info("Send checkpointmark of task {} / {}",  offloadingTask.taskId, checkpointMark);
          resultCollector.collector.emit(new KafkaOffloadingOutput(offloadingTask.taskId, 1, checkpointMark,
            checkpointMarkCoder, stateMap, stateAndCoderMap.right()));
        } else {
          final UnboundedSourceReadable readable = (UnboundedSourceReadable) srcDataFetcher.getReadable();
          final Coder<UnboundedSource.CheckpointMark> checkpointMarkCoder = readable.getUnboundedSource().getCheckpointMarkCoder();

          LOG.info("Send checkpointmark of task {}  / {} to vm",
            offloadingTask.taskId, readyTask.checkpointMark);
          resultCollector.collector.emit(new KafkaOffloadingOutput(
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
      resultCollector.collector.emit(new StateOutput(offloadingTask.taskId, stateMap, stateAndCoderMap.right()));
    }

    pendingFutures.clear();

    return true;
  }

  @Override
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
    outputfutures.addAll(pipeOutputWriters.stream()
      .map(outputWriter -> {
        return outputWriter.close(offloadingTask.taskId);
      }).collect(Collectors.toList()));

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

  public static List<NextIntraTaskOperatorInfo> getInternalMainOutputs(
    final IRVertex irVertex,
    final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
    final Map<Edge, Integer> edgeIndexMap,
    final Map<IRVertex, InputWatermarkManager> operatorWatermarkManagerMap) {

    return irVertexDag.getOutgoingEdgesOf(irVertex.getId())
      .stream()
      .filter(edge -> !edge.getPropertyValue(AdditionalOutputTagProperty.class).isPresent())
      .map(edge -> {
        final int index = edgeIndexMap.get(edge);
        final OperatorVertex nextOperator = (OperatorVertex) edge.getDst();
        final InputWatermarkManager inputWatermarkManager = operatorWatermarkManagerMap.get(nextOperator);
        return new NextIntraTaskOperatorInfo(index, edge, nextOperator, inputWatermarkManager);
      })
      .collect(Collectors.toList());
  }

  private static Map<String, List<NextIntraTaskOperatorInfo>> getInternalAdditionalOutputMap(
    final IRVertex irVertex,
    final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
    final Map<Edge, Integer> edgeIndexMap,
    final Map<IRVertex, InputWatermarkManager> operatorWatermarkManagerMap) {
    // Add all intra-task additional tags to additional output map.
    final Map<String, List<NextIntraTaskOperatorInfo>> map = new HashMap<>();

    irVertexDag.getOutgoingEdgesOf(irVertex.getId())
      .stream()
      .filter(edge -> edge.getPropertyValue(AdditionalOutputTagProperty.class).isPresent())
      .map(edge -> {
        final String outputTag = edge.getPropertyValue(AdditionalOutputTagProperty.class).get();
        final int index = edgeIndexMap.get(edge);
        final OperatorVertex nextOperator = (OperatorVertex) edge.getDst();
        final InputWatermarkManager inputWatermarkManager = operatorWatermarkManagerMap.get(nextOperator);
        return Pair.of(outputTag, new NextIntraTaskOperatorInfo(index, edge, nextOperator, inputWatermarkManager));
      })
      .forEach(pair -> {
        map.putIfAbsent(pair.left(), new ArrayList<>());
        map.get(pair.left()).add(pair.right());
      });

    return map;
  }

  public static Map<String, List<PipeOutputWriter>> getExternalAdditionalOutputMap(
    final IRVertex irVertex,
    final List<StageEdge> outEdgesToChildrenTasks,
    final IntermediateDataIOFactory intermediateDataIOFactory,
    final int taskIndex,
    final int originTaskIndex,
    final Map<String, Serializer> serializerMap,
    final Set<PipeOutputWriter> outputWriters,
    final String taskId,
    final RendevousServerClient client,
    final ExecutorThread et,
    final TaskMetrics taskMetrics) {
    // Add all inter-task additional tags to additional output map.
    final Map<String, List<PipeOutputWriter>> map = new HashMap<>();

    outEdgesToChildrenTasks
      .stream()
      .filter(edge -> edge.getSrcIRVertex().getId().equals(irVertex.getId()))
      .filter(edge -> edge.getPropertyValue(AdditionalOutputTagProperty.class).isPresent())
      .map(edge -> {
        final PipeOutputWriter outputWriter;

        // TODO fix
          outputWriter = intermediateDataIOFactory
            .createPipeWriter(taskIndex, originTaskIndex, edge, serializerMap, taskId, client, et, taskMetrics);


          outputWriters.add(outputWriter);

        final Pair<String, PipeOutputWriter> pair =
          Pair.of(edge.getPropertyValue(AdditionalOutputTagProperty.class).get(), outputWriter);
        return pair;
      })
      .forEach(pair -> {
        map.putIfAbsent(pair.left(), new ArrayList<>());
        map.get(pair.left()).add(pair.right());
      });

    return map;
  }

  public static List<PipeOutputWriter> getExternalMainOutputs(final IRVertex irVertex,
                                                              final List<StageEdge> outEdgesToChildrenTasks,
                                                              final IntermediateDataIOFactory intermediateDataIOFactory,
                                                              final int taskIndex,
                                                              final int originTaskIndex,
                                                              final Map<String, Serializer> serializerMap,
                                                              final Set<PipeOutputWriter> pipeOutputWriters,
                                                              final String taskId,
                                                              final RendevousServerClient client,
                                                              final ExecutorThread et,
                                                              final TaskMetrics taskMetrics) {
    return outEdgesToChildrenTasks
      .stream()
      .filter(edge -> edge.getSrcIRVertex().getId().equals(irVertex.getId()))
      .filter(edge -> !edge.getPropertyValue(AdditionalOutputTagProperty.class).isPresent())
      .map(outEdgeForThisVertex -> {
        LOG.info("Set expected watermark map for vertex {}", outEdgeForThisVertex.getDstIRVertex().getId());
          final PipeOutputWriter outputWriter = intermediateDataIOFactory
            .createPipeWriter(taskIndex,
              originTaskIndex, outEdgeForThisVertex, serializerMap,taskId,  client, et, taskMetrics);
        pipeOutputWriters.add(outputWriter);
        return outputWriter;
      })
      .collect(Collectors.toList());
  }

  private boolean allPendingDone() {
    for (final Future<Integer> pendingFuture : pendingFutures) {
      if (!pendingFuture.isDone()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public PendingState getPendingStatus() {
    throw new RuntimeException("Not supported");
  }

  @Override
  public boolean isFinished() {
    return finished && allPendingDone() && executorThread.queue.isEmpty();
  }

  @Override
  public void setOffloadedTaskTime(long t) {

  }

  @Override
  public AtomicLong getTaskExecutionTime() {
    return null;
  }

  @Override
  public OutputCollector getVertexOutputCollector(String vertexId) {
    return null;
  }

  @Override
  public long calculateOffloadedTaskTime() {
    return 0;
  }

  @Override
  public long getThreadId() {
    return 0;
  }

  @Override
  public boolean isRunning() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isOffloadPending() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isOffloaded() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isDeoffloadPending() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getId() {
    return offloadingTask.taskId;
  }

  private boolean hasEventInFetchers() {
    for (final DataFetcher fetcher : allFetchers) {
      if (fetcher.isAvailable()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean isStateless() {
    return false;
  }

  @Override
  public AtomicInteger getProcessedCnt() {
    throw new UnsupportedOperationException();
  }

  @Override
  public AtomicLong getPrevOffloadStartTime() {
    throw new UnsupportedOperationException();
  }

  @Override
  public AtomicLong getPrevOffloadEndTime() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void startOffloading(long baseTime, Object worker, EventHandler<Integer> doneHandler) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void endOffloading(EventHandler<Integer> m) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void execute() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void sendToServerless(Object event, List<String> nextOperatorIds, long wm, String edgeId) {
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
  public ExecutorThread getExecutorThread() {
    return executorThread;
  }

  @Override
  public boolean isSource() {
    return sourceVertexDataFetchers.size() > 0;
  }

  @Override
  public void handleIntermediateWatermarkEvent(final Object element, final DataFetcher dataFetcher) {
    executorThread.decoderThread.execute(() -> {
      executorThread.queue.add(() -> {
        onEventFromDataFetcher(element, dataFetcher);
      });
    });
  }

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

  @Override
  public void handleOffloadingEvent(Object data) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setIRVertexPutOnHold(IRVertex irVertex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString() {
    return getId();
  }
}
