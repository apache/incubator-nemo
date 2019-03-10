package org.apache.nemo.runtime.executor.task;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.nemo.common.Pair;
import org.apache.nemo.runtime.executor.common.Serializer;
import org.apache.nemo.common.dag.Edge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.offloading.common.ServerlessExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;

public final class OperatorMetricCollector {
  private static final Logger LOG = LoggerFactory.getLogger(OperatorMetricCollector.class.getName());

  long prevWindowTime = System.currentTimeMillis();
  final long windowsize = 2000;
  long adjustTime;

  public int emittedCnt;
  public final IRVertex irVertex;
  private final EvalConf evalConf;
  public final List<IRVertex> dstVertices;

  private long prevFlushTime;
  private ByteBuf inputBuffer;
  private ByteBufOutputStream bos;
  public int serializedCnt;
  private final Serializer serializer;
  private final Edge edge;

  private ServerlessExecutorService serverlessExecutorService;

  public boolean isOffloading = false;

  // processed events - key: timestamp, value: processed events
  public final List<Pair<Long, Long>> processedEvents;

  public OperatorMetricCollector(final IRVertex srcVertex,
                                 final List<IRVertex> dstVertices,
                                 final Serializer serializer,
                                 final Edge edge,
                                 final EvalConf evalConf,
                                 final ExecutorService shutdownExecutor) {
    this.irVertex = srcVertex;
    this.serializedCnt = 0;
    this.dstVertices = dstVertices;
    this.evalConf = evalConf;
    this.serializer = serializer;
    this.edge = edge;
    this.processedEvents = new LinkedList<>();
    this.inputBuffer = PooledByteBufAllocator.DEFAULT.buffer();
    this.bos = new ByteBufOutputStream(inputBuffer);

    //LOG.info("SrcVertex: {}, DstVertices: {}, Edge: {}", srcVertex, dstVertices, edge);
  }

  public void setServerlessExecutorService(final ServerlessExecutorService sls) {
    serverlessExecutorService = sls;
  }

  @Override
  public String toString() {
    return irVertex.getId();
  }

  private void checkSink() {
    if (edge == null){
      throw new RuntimeException("This is sink!!");
    }
  }

  public void startOffloading() {
    LOG.info("OPeratorMetricCollector startOffloading");
    checkSink();

    prevFlushTime = System.currentTimeMillis();
    serializedCnt = 0;
    isOffloading = true;
  }

  public void endOffloading() {
    checkSink();

    LOG.info("End of offloading vertex {}", irVertex.getId());
    if (inputBuffer.readableBytes() > 0) {
      // TODO: send remaining data to serverless
      flushToServerless();
      inputBuffer = PooledByteBufAllocator.DEFAULT.buffer();
      bos = new ByteBufOutputStream(inputBuffer);
    }
    LOG.info("End of offloading vertex  -- end {}", irVertex.getId());
    serializedCnt = 0;
    isOffloading = false;
  }

  private boolean isFlusheable(final long curTime) {
    if (serverlessExecutorService == null || serverlessExecutorService.isShutdown()) {
      throw new RuntimeException("Serverless executor is null or shutdowned: " + serverlessExecutorService);
    }

    return  (inputBuffer.readableBytes() > evalConf.flushBytes
      || serializedCnt > evalConf.flushCount
      || curTime - prevFlushTime > evalConf.flushPeriod);
  }

  private void flushToServerless() {
    LOG.info("Flush to serverless in vertex {}: {}", irVertex.getId(), serializedCnt);
    final CompositeByteBuf compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeBuffer(2);
    final ByteBuf lengthBuf = PooledByteBufAllocator.DEFAULT.buffer(4);
    lengthBuf.writeInt(serializedCnt);
    compositeByteBuf.addComponents(true, lengthBuf, inputBuffer);
    // execute

    serverlessExecutorService.execute(compositeByteBuf);
  }

  public void sendToServerless(final Object event, final List<String> nextOperatorIds) {
    checkSink();

    //final Serializer serializer = serializerManager.getSerializer(dataFetcher.edge.getId());

    //LOG.info("Send from {}/{} to serverless, cnt: {}", id, dataFetcher.edge.getId(),
    // serializedCnt);
    //LOG.info("Offload from {} to {}", irVertex.getId(), nextOperatorIds);

    try {
      bos.writeInt(nextOperatorIds.size());
      for (int i = 0; i < nextOperatorIds.size(); i++) {
        bos.writeUTF(nextOperatorIds.get(i));
      }
      bos.writeUTF(edge.getId());
      serializer.getEncoderFactory().create(bos).encode(event);
      serializedCnt += 1;

      final long curTime = System.currentTimeMillis();
      if (isFlusheable(curTime)) {
        //if (serializedCnt > 10) {

        // flush
        flushToServerless();
        prevFlushTime = curTime;

        // reset
        inputBuffer = PooledByteBufAllocator.DEFAULT.buffer();
        bos = new ByteBufOutputStream(inputBuffer);
        serializedCnt = 0;
      }
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  //private final LatencyAndCnt latencyAndCnt = new LatencyAndCnt();
  private final List<Long> latencies = new LinkedList<>();

  public void setAdjustTime(final long adjTime) {
    adjustTime = adjTime;
  }

  private void avgLatencies(final long startTimestamp) {
    final long currTime = System.currentTimeMillis();
    final long latency = (currTime - startTimestamp) - adjustTime;
    latencies.add(latency);

    //latencyAndCnt.latencySum += latency;
    //latencyAndCnt.count += 1;

    if (currTime - prevWindowTime >= windowsize) {
      // logging!
      final int p95Index = Math.min(latencies.size() - 1, (int) (latencies.size() * 0.95));
      final int p99Index = Math.min(latencies.size() - 1, (int) (latencies.size() * 0.99));

      final long avg =  latencies.stream().reduce(0L, (x, y) -> x + y) / latencies.size();

      latencies.sort(Long::compareTo);
      final long p95 = latencies.get(p95Index);
      final long p99 = latencies.get(p99Index);
      LOG.info("Avg Latency {} P95: {}, P99: {}, Max: {} from vertex {}, processCnt {}",
        avg, p95, p99, latencies.get(latencies.size() - 1), irVertex.getId(), latencies.size());
      latencies.clear();

      prevWindowTime = currTime;
    }
  }

  public void processDone(final long startTimestamp) {
   final long currTime = System.currentTimeMillis();
    final long latency = (currTime - startTimestamp) - adjustTime;
    LOG.info("Event Latency {}", latency);
  }

  class LatencyAndCnt {
    public long latencySum = 0;
    public int count = 0;
  }
}
