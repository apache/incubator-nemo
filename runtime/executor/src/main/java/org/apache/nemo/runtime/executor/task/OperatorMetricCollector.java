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
import java.util.*;

public final class OperatorMetricCollector {
  private static final Logger LOG = LoggerFactory.getLogger(OperatorMetricCollector.class.getName());

  long prevWindowTime = System.currentTimeMillis();
  final long windowsize = 2000;
  long adjustTime;

  public int emittedCnt = 0;
  public final IRVertex irVertex;
  private final EvalConf evalConf;
  public final List<IRVertex> dstVertices;

  private ByteBuf inputBuffer;
  private ByteBufOutputStream bos;
  public int serializedCnt;
  private final Serializer serializer;
  private final Edge edge;

  private ServerlessExecutorService serverlessExecutorService;

  public boolean isOffloading = false;

  // processed events - key: timestamp, value: processed events
  public final List<Pair<Long, Long>> processedEvents;

  private long watermark;

  private final Map<Long, Integer> watermarkCounterMap;

  private final boolean isMonitor;

  private final List<Integer> latencyList = new LinkedList<>();

  private int expectedCnt;

  public OperatorMetricCollector(final IRVertex srcVertex,
                                 final List<IRVertex> dstVertices,
                                 final Serializer serializer,
                                 final Edge edge,
                                 final EvalConf evalConf,
                                 final Map<Long, Integer> watermarkCounterMap,
                                 final Set<String> monitoringVertices) {
    this.irVertex = srcVertex;
    this.serializedCnt = 0;
    this.dstVertices = dstVertices;
    this.evalConf = evalConf;
    this.serializer = serializer;
    this.edge = edge;
    this.expectedCnt = evalConf.samplingCnt;
    this.watermarkCounterMap = watermarkCounterMap;
    this.processedEvents = new LinkedList<>();
    this.inputBuffer = PooledByteBufAllocator.DEFAULT.buffer();
    this.bos = new ByteBufOutputStream(inputBuffer);
    this.isMonitor = monitoringVertices.contains(srcVertex.getId());

    LOG.info("Monitoring {} {}", srcVertex, isMonitor);
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
    serializedCnt = 0;
    isOffloading = true;
  }

  public void endOffloading() {
    checkSink();

    LOG.info("End of offloading vertex {}", irVertex.getId());
    if (inputBuffer.readableBytes() > 0) {
      // TODO: send remaining data to serverless
      flushToServerless();
    }
    LOG.info("End of offloading vertex  -- end {}", irVertex.getId());
    serializedCnt = 0;
    isOffloading = false;
  }

  private boolean isFlusheable() {
    if (serverlessExecutorService == null || serverlessExecutorService.isShutdown()) {
      throw new RuntimeException("Serverless executor is null or shutdowned: " + serverlessExecutorService);
    }

    return  (inputBuffer.readableBytes() > evalConf.flushBytes
      || serializedCnt > evalConf.flushCount);
  }

  public boolean hasFlushableData() {
    return inputBuffer.readableBytes() > 0;
  }

  public void flushToServerless() {
    final CompositeByteBuf compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeBuffer(2);
    final ByteBuf lengthBuf = PooledByteBufAllocator.DEFAULT.buffer(12);
    lengthBuf.writeInt(serializedCnt);
    lengthBuf.writeLong(watermark);

    LOG.info("Flush to serverless in vertex {}, watermark: {}: {}", irVertex.getId(), serializedCnt,
      watermark);
    watermarkCounterMap.put(watermark,
      watermarkCounterMap.getOrDefault(watermark, 0) + 1);

    compositeByteBuf.addComponents(true, lengthBuf, inputBuffer);
    // execute

    serverlessExecutorService.execute(compositeByteBuf);

    // reset
    inputBuffer = PooledByteBufAllocator.DEFAULT.buffer();
    bos = new ByteBufOutputStream(inputBuffer);
    serializedCnt = 0;
  }

  public void sendToServerless(final Object event,
                               final List<String> nextOperatorIds,
                               final long wm) {
    watermark = wm;
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

      if (isFlusheable()) {
        // flush
        flushToServerless();
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
    if (isMonitor) {
      final long currTime = System.currentTimeMillis();
      final int latency = (int)((currTime - startTimestamp) - adjustTime);
      latencyList.add(latency);
    }
  }

  // TODO: trigger this function
  public void flushLatencies() {
    if (latencyList.isEmpty()) {
      // accumulate the expected cnt
      expectedCnt += evalConf.samplingCnt;
    } else {
      final long cTime = System.currentTimeMillis();
      final int cnt = Math.min(1, latencyList.size());
      final double samplingRate = Math.max(1.0, expectedCnt / (double) cnt);
      final Random random = new Random(cTime);

      final Iterator<Integer> iterator = latencyList.iterator();
      while (iterator.hasNext() && expectedCnt > 0) {
        final Integer latency = iterator.next();
        if (random.nextDouble() < samplingRate) {
          LOG.info("Event Latency {} from {} expectedCnt: {}", latency, irVertex.getId(), expectedCnt);
          expectedCnt -= 1;
          iterator.remove();
        }
      }

      if (expectedCnt == 0) {
        latencyList.clear();
      }

      expectedCnt += evalConf.samplingCnt;
    }
  }

  class LatencyAndCnt {
    public long latencySum = 0;
    public int count = 0;
  }
}
