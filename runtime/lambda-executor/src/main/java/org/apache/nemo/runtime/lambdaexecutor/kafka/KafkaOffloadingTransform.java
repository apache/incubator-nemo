package org.apache.nemo.runtime.lambdaexecutor.kafka;

import avro.shaded.com.google.common.collect.Lists;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.Edge;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.ir.edge.executionproperty.AdditionalOutputTagProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.compiler.frontend.beam.source.BeamUnboundedSourceVertex;
import org.apache.nemo.compiler.frontend.beam.source.UnboundedSourceReadable;
import org.apache.nemo.offloading.common.OffloadingOutputCollector;
import org.apache.nemo.offloading.common.OffloadingTransform;
import org.apache.nemo.runtime.executor.common.*;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingResultCollector;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingTransformContextImpl;
import org.apache.nemo.runtime.lambdaexecutor.datatransfer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public final class KafkaOffloadingTransform<O> implements OffloadingTransform<KafkaOffloadingInput, O> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaOffloadingTransform.class.getName());

  private final String executorId;
  private final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag;
  private final Map<String, List<String>> taskOutgoingEdges;

  // key: data fetcher id, value: head operator
  private transient Map<String, KafkaOperatorVertexOutputCollector> outputCollectorMap;
  private transient Map<String, NextIntraTaskOperatorInfo> operatorVertexMap;
  private transient OffloadingResultCollector resultCollector;

  private transient HandleDataFetcher dataFetcherExecutor;
  final List<DataFetcher> dataFetchers = new ArrayList<>();

  // next stage address
  private final Map<String, Serializer> serializerMap;
  private final Map<String, InetSocketAddress> executorAddressMap;
  private final Map<Integer, String> dstTaskIndexTargetExecutorMap;
  private final List<StageEdge> stageEdges;
  private final Map<String, Double> samplingMap;

  private transient OffloadingContext offloadingContext;
  private transient OffloadingOutputCollector offloadingOutputCollector;
  private final int originTaskIndex;

  // TODO: we should get checkpoint mark in constructor!
  public KafkaOffloadingTransform(final String executorId,
                                  final int originTaskIndex,
                                  final Map<String, Double> samplingMap,
                                  final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag,
                                  final Map<String, List<String>> taskOutgoingEdges,
                                  final Map<String, InetSocketAddress> executorAddressMap,
                                  final Map<String, Serializer> serializerMap,
                                  final Map<Integer, String> dstTaskIndexTargetExecutorMap,
                                  final List<StageEdge> stageEdges) {
    this.executorId = executorId;
    this.originTaskIndex = originTaskIndex;
    this.irDag = irDag;
    this.samplingMap = samplingMap;
    this.taskOutgoingEdges = taskOutgoingEdges;
    this.executorAddressMap = executorAddressMap;
    this.serializerMap = serializerMap;
    this.dstTaskIndexTargetExecutorMap = dstTaskIndexTargetExecutorMap;
    this.stageEdges = stageEdges;
  }

  @Override
  public void prepare(final OffloadingContext context,
    final OffloadingOutputCollector oc) {
    this.offloadingContext = context;
    this.offloadingOutputCollector = oc;
  }

  private void prep(final int taskIndex) {

    System.out.println("TaskIndex: " + taskIndex + ", ExecutorId: " + executorId);
    System.out.println("Executor address map: " + executorAddressMap);
    System.out.println("Stage edges: " + stageEdges);
    System.out.println("TaskIndexTargetExecutorMap: " + dstTaskIndexTargetExecutorMap);

    final IntermediateDataIOFactory intermediateDataIOFactory;
    if (stageEdges.size() > 0) {
      // create byte transport
      final NativeChannelImplementationSelector selector = new NativeChannelImplementationSelector();
      final LambdaControlFrameEncoder controlFrameEncoder = new LambdaControlFrameEncoder(executorId);
      final LambdaDataFrameEncoder dataFrameEncoder = new LambdaDataFrameEncoder();
      final LambdaByteTransportChannelInitializer initializer =
        new LambdaByteTransportChannelInitializer(controlFrameEncoder, dataFrameEncoder, executorId);
      final LambdaByteTransport byteTransport = new LambdaByteTransport(
        executorId, selector, initializer, executorAddressMap);
      final ByteTransfer byteTransfer = new ByteTransfer(byteTransport, executorId);
      final PipeManagerWorker pipeManagerWorker =
        new PipeManagerWorker(executorId, byteTransfer, dstTaskIndexTargetExecutorMap);
      intermediateDataIOFactory =
        new IntermediateDataIOFactory(pipeManagerWorker);
    } else {
      intermediateDataIOFactory = null;
    }

    this.outputCollectorMap = new HashMap<>();
    this.operatorVertexMap = new HashMap<>();
    System.out.println("Stateless offloading transform prepare");
    // Traverse in a reverse-topological order to ensure that each visited vertex's children vertices exist.
    final List<IRVertex> reverseTopologicallySorted = Lists.reverse(irDag.getTopologicalSort());
    resultCollector = new OffloadingResultCollector(offloadingOutputCollector);

    // Build a map for edge as a key and edge index as a value
    // This variable is used for creating NextIntraTaskOperatorInfo
    // in {@link this#getInternalMainOutputs and this#internalMainOutputs}
    final AtomicInteger sourceCnt = new AtomicInteger(0);
    final Map<Edge, Integer> edgeIndexMap = new HashMap<>();
    reverseTopologicallySorted.forEach(childVertex -> {

      if (childVertex instanceof BeamUnboundedSourceVertex) {
        sourceCnt.getAndIncrement();
      }

      final List<Edge> edges = getAllIncomingEdges(irDag, childVertex);
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
        final List<Edge> edges = getAllIncomingEdges(irDag, childVertex);
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
        getInternalAdditionalOutputMap(irVertex, irDag, edgeIndexMap, operatorWatermarkManagerMap);

      // Main outputs
      final List<NextIntraTaskOperatorInfo> internalMainOutputs =
        getInternalMainOutputs(irVertex, irDag, edgeIndexMap, operatorWatermarkManagerMap);

      for (final List<NextIntraTaskOperatorInfo> interOps : internalAdditionalOutputMap.values()) {
        for (final NextIntraTaskOperatorInfo interOp : interOps) {
          operatorVertexMap.put(interOp.getNextOperator().getId(), interOp);
        }
      }

      final Map<String, List<PipeOutputWriter>> externalAdditionalOutputMap =
        getExternalAdditionalOutputMap(
          irVertex, stageEdges, intermediateDataIOFactory, taskIndex, originTaskIndex, serializerMap);

      final List<PipeOutputWriter> externalMainOutputs = getExternalMainOutputs(
        irVertex, stageEdges, intermediateDataIOFactory, taskIndex, originTaskIndex, serializerMap);



      for (final NextIntraTaskOperatorInfo interOp : internalMainOutputs) {
        operatorVertexMap.put(interOp.getNextOperator().getId(), interOp);
      }

      final boolean isSink = irVertex.isSink;
      // skip sink
      System.out.println("vertex " + irVertex.getId() + " outgoing edges: " + irDag.getOutgoingEdgesOf(irVertex)
        + ", isSink: " + isSink);
      KafkaOperatorVertexOutputCollector outputCollector =
        new KafkaOperatorVertexOutputCollector(
          irVertex,
          samplingMap.getOrDefault(irVertex.getId(), 1.0),
          irVertex.isSink ? null : irDag.getOutgoingEdgesOf(irVertex).get(0), /* just use first edge for encoding */
          internalMainOutputs,
          internalAdditionalOutputMap,
          resultCollector,
          outputCollectorMap,
          taskOutgoingEdges,
          externalAdditionalOutputMap,
          externalMainOutputs);

      outputCollectorMap.put(irVertex.getId(), outputCollector);

      // get source
      if (irVertex instanceof BeamUnboundedSourceVertex) {
        final BeamUnboundedSourceVertex beamUnboundedSourceVertex = (BeamUnboundedSourceVertex) irVertex;
        final RuntimeEdge edge = irDag.getOutgoingEdgesOf(irVertex).get(0);

        final SourceVertexDataFetcher dataFetcher = new SourceVertexDataFetcher(
          beamUnboundedSourceVertex, edge, null, outputCollector);
        dataFetchers.add(dataFetcher);
      }

      final Transform transform;
      if (irVertex instanceof OperatorVertex) {
        transform = ((OperatorVertex) irVertex).getTransform();
        transform.prepare(new OffloadingTransformContextImpl(irVertex), outputCollector);
      }

    });
  }

  // receive batch (list) data
  @Override
  public void onData(final KafkaOffloadingInput input) {
    // TODO: handle multiple data fetchers!!

    prep(input.taskIndex);

    final UnboundedSource.CheckpointMark checkpointMark = input.checkpointMark;
    final UnboundedSource unboundedSource = input.unboundedSource;
    LOG.info("Receive checkpointmark: {}", checkpointMark);

    final SourceVertexDataFetcher dataFetcher = (SourceVertexDataFetcher) dataFetchers.get(0);
    final BeamUnboundedSourceVertex beamUnboundedSourceVertex = (BeamUnboundedSourceVertex) dataFetcher.getDataSource();
    beamUnboundedSourceVertex.setUnboundedSource(unboundedSource);

    final UnboundedSourceReadable readable =
      new UnboundedSourceReadable(unboundedSource, null, checkpointMark);

    dataFetcher.setReadable(readable);

    dataFetcherExecutor = new HandleDataFetcher(input.id, dataFetchers, resultCollector, checkpointMark);
    dataFetcherExecutor.start();
  }

  @Override
  public void close() {
    try {
      dataFetcherExecutor.close();
      // TODO: we send checkpoint mark to vm
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }


  // Get all of the intra-task edges
  private static List<Edge> getAllIncomingEdges(
    final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
    final IRVertex childVertex) {
    final List<Edge> edges = new ArrayList<>();
    edges.addAll(irVertexDag.getIncomingEdgesOf(childVertex));
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
    final Map<String, Serializer> serializerMap) {
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
                                                          final Map<String, Serializer> serializerMap) {
    return outEdgesToChildrenTasks
      .stream()
      .filter(edge -> edge.getSrcIRVertex().getId().equals(irVertex.getId()))
      .filter(edge -> !edge.getPropertyValue(AdditionalOutputTagProperty.class).isPresent())
      .map(outEdgeForThisVertex -> {
        LOG.info("Set expected watermark map for vertex {}", outEdgeForThisVertex.getDstIRVertex().getId());
          final PipeOutputWriter outputWriter = intermediateDataIOFactory
            .createPipeWriter(taskIndex, originTaskIndex, outEdgeForThisVertex, serializerMap);
        return outputWriter;
      })
      .collect(Collectors.toList());
  }
}
