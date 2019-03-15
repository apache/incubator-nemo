package org.apache.nemo.runtime.executor.task;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.Edge;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.offloading.common.ServerlessExecutorProvider;
import org.apache.nemo.offloading.common.ServerlessExecutorService;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.runtime.executor.common.NextIntraTaskOperatorInfo;
import org.apache.nemo.runtime.executor.data.SerializerManager;
import org.apache.nemo.runtime.executor.datatransfer.OperatorVertexOutputCollector;
import org.apache.nemo.runtime.executor.datatransfer.OutputWriter;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingResultEvent;
import org.apache.nemo.runtime.lambdaexecutor.StatelessOffloadingSerializer;
import org.apache.nemo.runtime.lambdaexecutor.StatelessOffloadingTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public final class OffloadingContext {
  private static final Logger LOG = LoggerFactory.getLogger(OffloadingContext.class.getName());

  private final String taskId;
  private final byte[] serializedDag;
  private final Collection<Pair<OperatorMetricCollector, OutputCollector>> burstyOperators;
  private final ConcurrentLinkedQueue<Object> offloadingEventQueue;


  private ServerlessExecutorService serverlessExecutorService;

  private final ExecutorService shutdownExecutor = Executors.newSingleThreadExecutor();
  private final ServerlessExecutorProvider serverlessExecutorProvider;
  private final Map<String, List<String>> taskOutgoingEdges;
  private final SerializerManager serializerManager;
  final Map<String, Pair<OperatorMetricCollector, OutputCollector>> vertexIdAndCollectorMap;
  final Map<String, OutputWriter> outputWriterMap;
  final Map<String, NextIntraTaskOperatorInfo> operatorInfoMap;
  final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag;

  private Map<String, List<String>> sourceAndSinkMap;

  private List<Pair<OperatorMetricCollector, OutputCollector>> offloadingHead;

  private boolean isStarted = false;
  private boolean finished = false;

  private final ScheduledExecutorService flusher = Executors.newSingleThreadScheduledExecutor();
  private final EvalConf evalConf;

  public OffloadingContext(
    final String taskId,
    final ConcurrentLinkedQueue<Object> offloadingEventQueue,
    final Collection<Pair<OperatorMetricCollector, OutputCollector>> burstyOperators,
    final ServerlessExecutorProvider serverlessExecutorProvider,
    final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
    final byte[] serializedDag,
    final Map<String, List<String>> taskOutgoingEdges,
    final SerializerManager serializerManager,
    final Map<String, Pair<OperatorMetricCollector, OutputCollector>> vertexIdAndCollectorMap,
    final Map<String, OutputWriter> outputWriterMap,
    final Map<String, NextIntraTaskOperatorInfo> operatorInfoMap,
    final EvalConf evalConf) {
    this.taskId = taskId;
    this.offloadingEventQueue = offloadingEventQueue;
    this.burstyOperators = burstyOperators;
    this.irVertexDag = irVertexDag;
    this.serializedDag = serializedDag;
    this.serverlessExecutorProvider = serverlessExecutorProvider;
    this.taskOutgoingEdges = taskOutgoingEdges;
    this.serializerManager = serializerManager;
    this.vertexIdAndCollectorMap = vertexIdAndCollectorMap;
    this.outputWriterMap = outputWriterMap;
    this.operatorInfoMap = operatorInfoMap;
    this.evalConf = evalConf;
  }

  public void startOffloading() {

    if (isStarted) {
      throw new RuntimeException("The offloading is already started");
    }

    isStarted = true;
    sourceAndSinkMap = new HashMap<>();

    if (!burstyOperators.isEmpty()) {
      // 1) remove stateful

      // build DAG
      final DAG<IRVertex, Edge<IRVertex>> copyDag = SerializationUtils.deserialize(serializedDag);

      burstyOperators.stream().forEach(pair -> {
        final IRVertex vertex = pair.left().irVertex;
        copyDag.getOutgoingEdgesOf(vertex).stream().forEach(edge -> {
          // this edge can be offloaded
          if (!edge.getDst().isSink && !edge.getDst().isStateful) {
            edge.getDst().isOffloading = true;
          } else {
            edge.getDst().isOffloading = false;
          }
        });
      });

      serverlessExecutorService = serverlessExecutorProvider.
        newCachedPool(new StatelessOffloadingTransform(copyDag, taskOutgoingEdges),
          new StatelessOffloadingSerializer(serializerManager.runtimeEdgeIdToSerializer),
          new StatelessOffloadingEventHandler(offloadingEventQueue));

      final List<Pair<OperatorMetricCollector, OutputCollector>> ops = new ArrayList<>(burstyOperators.size());
      for (final Pair<OperatorMetricCollector, OutputCollector> op : burstyOperators) {
        ops.add(op);
      }

      LOG.info("Collecting burstyOps at {}", taskId);

      final StringBuilder sb = new StringBuilder();
      sb.append(String.format("Offloading dag at task %s\n: ", taskId));
      for (final IRVertex vertex : copyDag.getVertices()) {
        sb.append(String.format("%s is offloading %s, stateful %s, isSink %s\n",
          vertex.getId(), vertex.isOffloading, vertex.isStateful, vertex.isSink));
      }

      LOG.info(sb.toString());

      // find header
      offloadingHead = findHeader(copyDag, ops);

      for (final Pair<OperatorMetricCollector, OutputCollector> pair : offloadingHead) {
        // find sink that can reach from the headers
        final List<String> sinks = new ArrayList<>();
        findOffloadingSink(pair.left().irVertex, copyDag, sinks);
        sourceAndSinkMap.put(pair.left().irVertex.getId(), sinks);

        LOG.info("Header operator: {}, sinks: {}", pair.left().irVertex, sinks);
        final OperatorMetricCollector omc = pair.left();
        if (!omc.irVertex.isSink) {
          omc.setServerlessExecutorService(serverlessExecutorService);

          // Send start message
          offloadingEventQueue.add(new OffloadingControlEvent(
            OffloadingControlEvent.ControlMessageType.START_OFFLOADING, omc.irVertex.getId(), this));
        }
      }

      flusher.scheduleAtFixedRate(() -> {
        try {
          if (!finished) {
            flush();
          }
        } catch (final Exception e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }, evalConf.flushPeriod, evalConf.flushPeriod,TimeUnit.MILLISECONDS);
    }
  }

  private void flush() {
    if (!finished) {
      for (final Pair<OperatorMetricCollector, OutputCollector> pair : offloadingHead) {
        offloadingEventQueue.add(new OffloadingControlEvent(
          OffloadingControlEvent.ControlMessageType.FLUSH, pair.left().irVertex.getId()));
      }
    }
  }

  public void endOffloading() {
    if (!isStarted) {
      throw new RuntimeException("The offloading is not started!");
    }

    if (finished) {
      throw new RuntimeException("The offloading is already finishsed!");
    }

    finished = true;
    flusher.shutdown();
    try {
      flusher.awaitTermination(3000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }


    LOG.info("End offloading!");
    // Do sth for offloading end

    for (final Pair<OperatorMetricCollector, OutputCollector> pair : offloadingHead) {
      if (!pair.left().irVertex.isSink) {
        LOG.info("Disable offloading {}", pair.left().irVertex.getId());

        // Send stop message
        offloadingEventQueue.add(new OffloadingControlEvent(
          OffloadingControlEvent.ControlMessageType.STOP_OFFLOADING, pair.left().irVertex.getId()));
      }
    }

    // waiting for disable offloading
    for (final Pair<OperatorMetricCollector, OutputCollector> pair : offloadingHead) {
      if (!pair.left().irVertex.isSink) {
        while (pair.left().isOffloading) {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    }

    LOG.info("Shutting down operator of header {}", offloadingHead.stream().map(pair -> pair.left().irVertex)
      .collect(Collectors.toList()));

    shutdownExecutor.execute(() -> {
      serverlessExecutorService.shutdown();
    });
  }

  public boolean isFinished() {
    return serverlessExecutorService.isFinished();
  }

  public List<String> getOffloadingSinks(final IRVertex src) {
    return sourceAndSinkMap.get(src.getId());
  }

  private void findOffloadingSink(final IRVertex curr,
                                  final DAG<IRVertex, Edge<IRVertex>> dag,
                                  final List<String> sinks) {
    for (final Edge<IRVertex> nextEdge : dag.getOutgoingEdgesOf(curr)) {
      if (!nextEdge.getDst().isOffloading) {
        sinks.add(nextEdge.getDst().getId());
      } else {
        findOffloadingSink(nextEdge.getDst(), dag, sinks);
      }
    }

    if (curr.isOffloading) {
      if (taskOutgoingEdges.containsKey(curr.getId())) {
        for (final String nextDst : taskOutgoingEdges.get(curr.getId())) {
          sinks.add(nextDst);
        }
      }
    }
  }

  private List<Pair<OperatorMetricCollector, OutputCollector>> findHeader(
    final DAG<IRVertex, Edge<IRVertex>> dag,
    final List<Pair<OperatorMetricCollector, OutputCollector>> burstyOperators) {

    final List<String> burstyOps = burstyOperators.stream()
      .map(pair -> pair.left().irVertex.getId()).collect(Collectors.toList());

    final List<IRVertex> possibleHeaders = dag.getVertices().stream()
      .filter(vertex -> vertex.isOffloading)
      .collect(Collectors.toList());

    final List<String> possibleHeaderStrs = dag.getVertices().stream()
      .filter(vertex -> vertex.isOffloading)
      .map(vertex -> vertex.getId())
      .collect(Collectors.toList());

    final List<Boolean> headers = new ArrayList<>(possibleHeaders.size());
    for (int i = 0; i < possibleHeaders.size(); i++) {
      headers.add(true);
    }

    LOG.info("Possible headers: {}", possibleHeaders);

    for (final IRVertex possibleHeader : possibleHeaders) {
      final List<RuntimeEdge<IRVertex>> edges = irVertexDag.getOutgoingEdgesOf(possibleHeader);
      edges.stream().forEach((edge) -> {
        LOG.info("Header] Edge {} -> {}", edge.getSrc().getId(), edge.getDst().getId());
        final int index = possibleHeaderStrs.indexOf(edge.getDst().getId());
        if (index >= 0) {
          // dst is not a header
          headers.set(index, false);
        }
      });
    }

    final List<Pair<OperatorMetricCollector, OutputCollector>> l = new ArrayList<>(possibleHeaders.size());
    for (int i = 0; i < possibleHeaders.size(); i++) {
      if (headers.get(i)) {
        l.add(vertexIdAndCollectorMap.get(possibleHeaderStrs.get(i)));
      }
    }

    LOG.info("Header] before adjusting the header: {}",
      l.stream().map(pair -> pair.left()).collect(Collectors.toList()));

    // Check incoming vertices
    final Iterator<Pair<OperatorMetricCollector, OutputCollector>> iterator = l.iterator();
    final List<Pair<OperatorMetricCollector, OutputCollector>> head = new LinkedList<>();
    while (iterator.hasNext()) {
      final Pair<OperatorMetricCollector, OutputCollector> h = iterator.next();
      for (final Edge<IRVertex> edge : dag.getIncomingEdgesOf(h.left().irVertex)) {
        if (burstyOps.contains(edge.getSrc().getId())) {
          iterator.remove();
          head.add(burstyOperators.get(burstyOps.indexOf(edge.getSrc().getId())));
        }
      }
    }

    l.addAll(head);

    LOG.info("Header] after adjusting the header: {}",
      l.stream().map(pair -> pair.left()).collect(Collectors.toList()));

    return l;
  }

}
