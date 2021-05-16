package org.apache.nemo.runtime.master;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class MetricStatistics {
  private static final Logger LOG = LoggerFactory.getLogger(RuntimeMaster.class.getName());

  private final DescriptiveStatistics latencyStatistics;
  private static final long TIME_WINDOW = 1000;
  private final ClientRPC clientRPC;

  @Inject
  public MetricStatistics(final ClientRPC clientRPC) {
    this.latencyStatistics = new DescriptiveStatistics();
    this.clientRPC = clientRPC;
  }

  private long collectionTime = System.currentTimeMillis();

  public void redirectPrepartion(final long elapsed) {
    LOG.info("Redirection preparation time {}", elapsed);
  }

  public void redirectionDone(final long elapsed) {
    LOG.info("Redirection done time {}", elapsed);
  }


  private final Map<String, Long> taskStopTimeMap = new ConcurrentHashMap<>();
  private final Map<String, Long> taskRescheduleTimeMap = new ConcurrentHashMap<>();
  private final Map<String, Long> taskRestartTimeMap = new ConcurrentHashMap<>();

  public void taskStopStart(final String taskId) {
    taskStopTimeMap.put(taskId, System.currentTimeMillis());
  }

  public void taskStopDone(final String taskId) {
    final long et = System.currentTimeMillis();
    if (taskStopTimeMap.containsKey(taskId)) {
      LOG.info("Task stop elapsed time {}", et - taskStopTimeMap.remove(taskId));
    }
    taskRescheduleTimeMap.put(taskId, et);
  }

  public void taskScheduleDone(final String taskId) {
    final long et = System.currentTimeMillis();
    if (taskRescheduleTimeMap.containsKey(taskId)) {
      LOG.info("Task reschedule elapsed time {}", et - taskRescheduleTimeMap.remove(taskId));
    }
    taskRestartTimeMap.put(taskId, et);
  }

  public void taskRestart(final String taskId) {
    final long et = System.currentTimeMillis();
    if (taskRestartTimeMap.containsKey(taskId)) {
      LOG.info("Task restart elapsed time {}", et - taskRestartTimeMap.remove(taskId));
    }
  }

  public synchronized void collectLatency(final long latency, final String executorId) {

    LOG.info("Latency in Master in {}: {}", executorId, latency);

    final long curr =  System.currentTimeMillis();
    if (curr - collectionTime >= TIME_WINDOW) {
      final String log = String.format("COLLECT_LATENCY\t" +
        "median\t%f\tp95\t%f\tp99\t%f\ttail\t%f",
        latencyStatistics.getPercentile(0.5),
        latencyStatistics.getPercentile(0.95),
        latencyStatistics.getPercentile(0.99),
        latencyStatistics.getMax());

      latencyStatistics.clear();
      latencyStatistics.addValue(latency);

      LOG.info(log);

      collectionTime = curr;

      clientRPC.send(ControlMessage.DriverToClientMessage.newBuilder()
        .setType(ControlMessage.DriverToClientMessageType.PrintLog)
        .setPrintStr(log).build());
    } else {
      latencyStatistics.addValue(latency);
    }
  }

}
