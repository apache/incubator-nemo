package org.apache.nemo.runtime.lambdaexecutor.general;

import avro.shaded.com.google.common.collect.Lists;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.Edge;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.ir.edge.executionproperty.AdditionalOutputTagProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.punctuation.Finishmark;
import org.apache.nemo.common.punctuation.TimestampAndValue;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.compiler.frontend.beam.source.BeamUnboundedSourceVertex;
import org.apache.nemo.compiler.frontend.beam.source.UnboundedSourceReadable;
import org.apache.nemo.offloading.common.OffloadingOutputCollector;
import org.apache.nemo.runtime.executor.common.*;
import org.apache.nemo.runtime.executor.common.datatransfer.DataFetcherOutputCollector;
import org.apache.nemo.runtime.executor.common.datatransfer.InputReader;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingResultCollector;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingTransformContextImpl;
import org.apache.nemo.runtime.lambdaexecutor.datatransfer.*;
import org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOffloadingOutput;
import org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOperatorVertexOutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public final class OffloadingTaskExecutor implements TaskExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(OffloadingTaskExecutor.class.getName());

  private final OffloadingTask offloadingTask;
  private final LambdaByteTransport byteTransport;
  private final PipeManagerWorker pipeManagerWorker;
  private final OffloadingOutputCollector oc;
  private final OffloadingResultCollector resultCollector;

  private final IntermediateDataIOFactory intermediateDataIOFactory;
  private final Map<String, KafkaOperatorVertexOutputCollector> outputCollectorMap;
  private final Map<String, NextIntraTaskOperatorInfo> operatorVertexMap;

  private final Map<String, InetSocketAddress> executorAddressMap;
  private final Map<String, Serializer> serializerMap;
  private final Map<Pair<RuntimeEdge, Integer>, String> taskExecutorIdMap;

  private final Set<PipeOutputWriter> pipeOutputWriters;

  private final List<DataFetcher> availableFetchers;
  private final List<DataFetcher> pendingFetchers;

  private final int pollingInterval = 200; // ms
  private boolean pollingTime = false;

  private final ScheduledExecutorService pollingTrigger;
  public final AtomicLong taskExecutionTime = new AtomicLong(0);

  // TODO: we should get checkpoint mark in constructor!
  public OffloadingTaskExecutor(final OffloadingTask offloadingTask,
                                final Map<String, InetSocketAddress> executorAddressMap,
                                final Map<String, Serializer> serializerMap,
                                final Map<Pair<RuntimeEdge, Integer>, String> taskExecutorIdMap,
                                final LambdaByteTransport byteTransport,
                                final PipeManagerWorker pipeManagerWorker,
                                final IntermediateDataIOFactory intermediateDataIOFactory,
                                final OffloadingOutputCollector oc,
                                final ScheduledExecutorService pollingTrigger) {
    this.offloadingTask = offloadingTask;
    this.serializerMap = serializerMap;
    this.taskExecutorIdMap = taskExecutorIdMap;
    this.executorAddressMap = executorAddressMap;
    this.byteTransport = byteTransport;
    this.pipeManagerWorker = pipeManagerWorker;
    this.intermediateDataIOFactory = intermediateDataIOFactory;
    this.oc = oc;
    this.outputCollectorMap = new HashMap<>();
    this.operatorVertexMap = new HashMap<>();
    this.pipeOutputWriters = new HashSet<>();
    this.resultCollector = new OffloadingResultCollector(oc);
    this.availableFetchers = new LinkedList<>();
    this.pendingFetchers = new LinkedList<>();
    this.pollingTrigger = pollingTrigger;


    pollingTrigger.scheduleAtFixedRate(() -> {
      pollingTime = true;
    }, pollingInterval, pollingInterval, TimeUnit.MILLISECONDS);

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

  private void prepare() {

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
              new OperatorWatermarkCollector((OperatorVertex) childVertex)));
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
          serializerMap, pipeOutputWriters);

      final List<PipeOutputWriter> externalMainOutputs = getExternalMainOutputs(
        irVertex, offloadingTask.outgoingEdges,
        intermediateDataIOFactory,
        offloadingTask.taskIndex,
        offloadingTask.taskIndex, serializerMap, pipeOutputWriters);

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

        final UnboundedSource.CheckpointMark checkpointMark = offloadingTask.checkpointMark;
        final UnboundedSource unboundedSource = offloadingTask.unboundedSource;
        LOG.info("Receive checkpointmark: {}", checkpointMark);

        beamUnboundedSourceVertex.setUnboundedSource(unboundedSource);

        final UnboundedSourceReadable readable =
          new UnboundedSourceReadable(unboundedSource, null, checkpointMark);

        final SourceVertexDataFetcher dataFetcher = new SourceVertexDataFetcher(
          beamUnboundedSourceVertex, edge, readable, outputCollector);
        availableFetchers.add(dataFetcher);
      }

      // task incoming edges!!
      offloadingTask.incomingEdges
        .stream()
        .filter(inEdge -> inEdge.getDstIRVertex().getId().equals(irVertex.getId()))
        .map(incomingEdge -> {

          LOG.info("Incoming edge: {}, taskIndex: {}, taskId: {}", incomingEdge, offloadingTask.taskIndex,
            offloadingTask.taskId);

          return Pair.of(incomingEdge, intermediateDataIOFactory
            .createReader(offloadingTask.taskIndex, incomingEdge.getSrcIRVertex(), incomingEdge));
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

            availableFetchers.add(
              new LambdaParentTaskDataFetcher(
                offloadingTask.taskId,
                parentTaskReader.getSrcIrVertex(),
                edge,
                parentTaskReader,
                dataFetcherOutputCollector));

          }
        });

      final Transform transform;
      if (irVertex instanceof OperatorVertex) {
        transform = ((OperatorVertex) irVertex).getTransform();
        transform.prepare(new OffloadingTransformContextImpl(irVertex), outputCollector);
      }
    });
  }

  // receive batch (list) data
  @Override
  public boolean handleData() {
    boolean dataProcessed = false;

    // We first fetch data from available data fetchers
    final Iterator<DataFetcher> availableIterator = availableFetchers.iterator();
    while (availableIterator.hasNext()) {

      final DataFetcher dataFetcher = availableIterator.next();
      try {
        //final long a = System.currentTimeMillis();
        final Object element = dataFetcher.fetchDataElement();

        //fetchTime += (System.currentTimeMillis() - a);

        //final long b = System.currentTimeMillis();
        onEventFromDataFetcher(element, dataFetcher);
        //processingTime += (System.currentTimeMillis() - b);
        dataProcessed = true;

        if (element instanceof Finishmark) {
          availableIterator.remove();
        }
      } catch (final NoSuchElementException e) {
        // No element in current data fetcher, fetch data from next fetcher
        // move current data fetcher to pending.
        availableIterator.remove();
        pendingFetchers.add(dataFetcher);
      } catch (final IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

    final Iterator<DataFetcher> pendingIterator = pendingFetchers.iterator();

    if (pollingTime) {
      // We check pending data every polling interval
      pollingTime = false;

      while (pendingIterator.hasNext()) {
        final DataFetcher dataFetcher = pendingIterator.next();
        try {
          //final long a = System.currentTimeMillis();
          final Object element = dataFetcher.fetchDataElement();
          //fetchTime += (System.currentTimeMillis() - a);

          //final long b = System.currentTimeMillis();
          onEventFromDataFetcher(element, dataFetcher);
          // processingTime += (System.currentTimeMillis() - b);

          // We processed data. This means the data fetcher is now available.
          // Add current data fetcher to available
          pendingIterator.remove();
          if (!(element instanceof Finishmark)) {
            availableFetchers.add(dataFetcher);
          }

        } catch (final NoSuchElementException e) {
          // The current data fetcher is still pending.. try next data fetcher
        } catch (final IOException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
    }

    return dataProcessed;
  }

  private void processWatermark(final OutputCollector outputCollector,
                                final Watermark watermark) {
    outputCollector.emitWatermark(watermark);
  }

  /**
   * Process a data element down the DAG dependency.
   */
  private void processElement(final OutputCollector outputCollector, final TimestampAndValue dataElement) {
    outputCollector.setInputTimestamp(dataElement.timestamp);
    outputCollector.emit(dataElement.value);
  }

  private void onEventFromDataFetcher(final Object event,
                                      final DataFetcher dataFetcher) {

    if (event instanceof Watermark) {
      // Watermark
      processWatermark(dataFetcher.getOutputCollector(), (Watermark) event);
    } else if (event instanceof TimestampAndValue) {
      // Process data element
      processElement(dataFetcher.getOutputCollector(), (TimestampAndValue) event);

    } else {
      throw new RuntimeException("Invalid type of event: " + event);
    }
  }

  @Override
  public void close() {

    final List<DataFetcher> allFetchers = new ArrayList<>();
    allFetchers.addAll(availableFetchers);
    allFetchers.addAll(pendingFetchers);

    for (final DataFetcher dataFetcher : allFetchers) {

      if (dataFetcher instanceof SourceVertexDataFetcher) {
        // send checkpoint mark to the VM!!
        final SourceVertexDataFetcher srcDataFetcher = (SourceVertexDataFetcher) dataFetcher;
        if (srcDataFetcher.isStarted()) {
          final UnboundedSourceReadable readable = (UnboundedSourceReadable) srcDataFetcher.getReadable();
          final UnboundedSource.CheckpointMark checkpointMark = readable.getReader().getCheckpointMark();
          LOG.info("Send checkpointmark of task {} / {}",  offloadingTask.taskId, checkpointMark);
          resultCollector.collector.emit(new KafkaOffloadingOutput(offloadingTask.taskId, 1, checkpointMark));
        } else {
          LOG.info("Send checkpointmark of task {}  / {} to vm",
            offloadingTask.taskId, offloadingTask.checkpointMark);
          resultCollector.collector.emit(new KafkaOffloadingOutput(
            offloadingTask.taskId, 1, offloadingTask.checkpointMark));
        }
      } else if (dataFetcher instanceof LambdaParentTaskDataFetcher) {
        // TODO: fix ..
        throw new RuntimeException("Not supported yet");
      }
    }


      // Close all data fetchers


      // flush transforms
      offloadingTask.irDag.getTopologicalSort().stream().forEach(irVertex -> {
        if (irVertex instanceof OperatorVertex) {
          final Transform transform = ((OperatorVertex) irVertex).getTransform();
          transform.flush();
        }
      });

    // TODO: close upstream data fetchers
    /*
      fetchers.forEach(fetcher -> {
        try {
          fetcher.close();
        } catch (final Exception e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      });
    */

    try {

      // TODO: fix
      for (final PipeOutputWriter outputWriter : pipeOutputWriters) {
        outputWriter.close();
      }

      Thread.sleep(3000);

      // TODO: we send checkpoint mark to vm
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
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
    final Set<PipeOutputWriter> outputWriters) {
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
            .createPipeWriter(taskIndex, originTaskIndex, edge, serializerMap);


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
                                                              final Set<PipeOutputWriter> pipeOutputWriters) {
    return outEdgesToChildrenTasks
      .stream()
      .filter(edge -> edge.getSrcIRVertex().getId().equals(irVertex.getId()))
      .filter(edge -> !edge.getPropertyValue(AdditionalOutputTagProperty.class).isPresent())
      .map(outEdgeForThisVertex -> {
        LOG.info("Set expected watermark map for vertex {}", outEdgeForThisVertex.getDstIRVertex().getId());
          final PipeOutputWriter outputWriter = intermediateDataIOFactory
            .createPipeWriter(taskIndex, originTaskIndex, outEdgeForThisVertex, serializerMap);
        pipeOutputWriters.add(outputWriter);
        return outputWriter;
      })
      .collect(Collectors.toList());
  }

  @Override
  public AtomicLong getTaskExecutionTime() {
    return taskExecutionTime;
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
  public void startOffloading(long baseTime) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void endOffloading() {
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

  @Override
  public void setIRVertexPutOnHold(IRVertex irVertex) {
    throw new UnsupportedOperationException();
  }
}
