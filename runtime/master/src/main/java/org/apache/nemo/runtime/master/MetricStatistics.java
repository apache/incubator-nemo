package org.apache.nemo.runtime.master;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

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

  public synchronized void collectLatency(final long latency, final String executorId) {
    latencyStatistics.addValue(latency);

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

      LOG.info(log);

      collectionTime = curr;

      clientRPC.send(ControlMessage.DriverToClientMessage.newBuilder()
        .setType(ControlMessage.DriverToClientMessageType.PrintLog)
        .setPrintStr(log).build());
    }
  }

}
