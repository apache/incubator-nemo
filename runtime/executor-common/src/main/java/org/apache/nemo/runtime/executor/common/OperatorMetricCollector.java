package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.message.MessageEnvironment;
import org.apache.nemo.runtime.message.PersistentConnectionToMasterMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public final class OperatorMetricCollector {
  private static final Logger LOG = LoggerFactory.getLogger(OperatorMetricCollector.class.getName());

  long prevWindowTime = System.currentTimeMillis();
  final long windowsize = 2000;
  long adjustTime;

  private final String vertexId;
  private final String executorId;
  private final String taskId;
  private final long latencyLimit;
  private final PersistentConnectionToMasterMap persistentConnectionToMasterMap;

  public OperatorMetricCollector(final String vertexId,
                                 final String executorId,
                                 final String taskId,
                                 final long latencyLimit,
                                 final PersistentConnectionToMasterMap persistentConnectionToMasterMap) {
    this.vertexId = vertexId;
    this.executorId = executorId;
    this.taskId = taskId;
    this.latencyLimit = latencyLimit;
    this.persistentConnectionToMasterMap = persistentConnectionToMasterMap;
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
        avg, p95, p99, latencies.get(latencies.size() - 1), "taskId", latencies.size());
      latencies.clear();

      prevWindowTime = currTime;
    }
  }

  int proceseedCnt = 0;
  long prevLogTime = System.currentTimeMillis();

  public void processDone(final long inputTimestamp,
                          final String vertexId,
                          final String executorId,
                          final String taskId,
                          final long latencyLimit,
                          final PersistentConnectionToMasterMap persistentConnectionToMasterMap) {

    final long currTime = System.currentTimeMillis();
    final int latency = (int) ((currTime - inputTimestamp));

    if (currTime - prevLogTime >= 500) {
      prevLogTime = currTime;
      LOG.info("Event Latency {} from {} in {}/{} ", latency,
        vertexId,
        executorId,
        taskId);

      if (latency > latencyLimit) {
        persistentConnectionToMasterMap
          .getMessageSender(MessageEnvironment.ListenerType.RUNTIME_MASTER_MESSAGE_LISTENER_ID)
          .send(ControlMessage.Message.newBuilder()
            .setId(RuntimeIdManager.generateMessageId())
            .setListenerId(MessageEnvironment.ListenerType.RUNTIME_MASTER_MESSAGE_LISTENER_ID.ordinal())
            .setType(ControlMessage.MessageType.LatencyCollection)
            .setLatencyMsg(ControlMessage.LatencyCollectionMessage.newBuilder()
              .setExecutorId(executorId)
              .setLatency(latency)
              .build())
            .build());
      }
    }
  }

  /*
  // TODO: trigger this function
  public void flushLatencies() {
    if (!latencyList.isEmpty()) {
      final long cTime = System.currentTimeMillis();
      final Random random = new Random(cTime);

      final int expectedCnt = Math.max(evalConf.samplingCnt,
        (int) (latencyList.size() * (evalConf.samplingCnt / 10000.0)));

      if (latencyList.size() <= expectedCnt) {
        for (final Integer latency : latencyList) {
          LOG.info("Event Latency {} from {} expectedCnt: {}", latency, irVertex.getId(), expectedCnt);
        }

        latencyList.clear();
      } else {
        // select!!
        final Set<Integer> selectedIndex = new HashSet<>();
        //LOG.info("Latency size: {}", latencyList.size());
        LOG.info("Expected count: {}, latency size: {} at {}", expectedCnt, latencyList.size(), irVertex.getId());

        while (selectedIndex.size() < expectedCnt) {
          final int index = random.nextInt(latencyList.size());
          if (!selectedIndex.contains(index)) {
            selectedIndex.add(index);
            LOG.info("Event Latency {} from {} expectedCnt: {}", latencyList.get(index), irVertex.getId());
          }
        }

        latencyList.clear();
      }
    }
  }
  */

  class LatencyAndCnt {
    public long latencySum = 0;
    public int count = 0;
  }
}
