package org.apache.nemo.runtime.master.backpressure;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.nemo.common.Util;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.conf.PolicyConf;
import org.apache.nemo.runtime.master.metric.ExecutorMetricInfo;
import org.apache.nemo.runtime.master.scaler.ExecutorMetricMap;
import org.apache.nemo.runtime.master.scheduler.ExecutorRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class InputAndQueueSizeBasedBackpressure implements Backpressure {
  private static final Logger LOG = LoggerFactory.getLogger(InputAndQueueSizeBasedBackpressure.class.getName());

  private final AtomicLong aggInput = new AtomicLong(0);
  private long currEmitInput = 0;

  private final ExecutorMetricMap executorMetricMap;
  private final PolicyConf policyConf;
  private final ExecutorRegistry executorRegistry;

  private final ScheduledExecutorService scheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor();

  private long currRate = Long.MAX_VALUE;

  private final DescriptiveStatistics avgCpuUse;
  private final DescriptiveStatistics avgInputRate;
  private final DescriptiveStatistics avgProcessingRate;
  private final DescriptiveStatistics avgQueueSize;
  private long currSourceEvent = 0;
  private long prevProcessingEvent = 0;

  private final int windowSize = 5;

  @Inject
  private InputAndQueueSizeBasedBackpressure(final ExecutorMetricMap executorMetricMap,
                                             final ExecutorRegistry executorRegistry,
                                             final PolicyConf policyConf) {
    this.executorMetricMap = executorMetricMap;
    this.policyConf = policyConf;
    this.executorRegistry = executorRegistry;
    this.avgCpuUse = new DescriptiveStatistics(windowSize);
    this.avgInputRate = new DescriptiveStatistics(windowSize);
    this.avgProcessingRate = new DescriptiveStatistics(windowSize);
    this.avgQueueSize = new DescriptiveStatistics(windowSize);
    this.currRate = policyConf.bpMinEvent;

    scheduledExecutorService.scheduleAtFixedRate(() -> {
      try {
        LOG.info("Current backpressure rate {}", currRate);

        final ExecutorMetricInfo info = executorMetricMap.getAggregated();

        // Calculate queue size
        final long queue = info.receiveEvent - info.processEvent;
        avgQueueSize.addValue(queue);

        // Update processing rate
        avgProcessingRate.addValue(info.processEvent - prevProcessingEvent);
        prevProcessingEvent = info.processEvent;

        if (info.numExecutor > 0) {
          avgCpuUse.addValue(info.cpuUse / info.numExecutor);
        }

        synchronized (this) {
          LOG.info("Total queue: {}, avg queue: {}, avg cpu: {}, currRate: {}, avgInputRate: {}," +
              "aggInput: {}, sourceEvent: {}, processingRate: {}, numExecutor: {}",
            queue, avgQueueSize.getMean(),
            avgCpuUse.getMean(), currRate, avgInputRate.getMean(),
            aggInput, currSourceEvent, avgProcessingRate.getMean(), info.numExecutor);

          if (queue > policyConf.bpQueueUpperBound) {
            // Back pressure
            if (queue > avgQueueSize.getMean()) {
              if (currRate > avgInputRate.getMean() && avgInputRate.getMean() > 1000) {
                currRate = (long) (avgInputRate.getMean() * policyConf.bpDecreaseRatio);
              } else {
                currRate *= policyConf.bpDecreaseRatio;
              }

              LOG.info("Decrease backpressure rate to {}", currRate);

              sendBackpressure(executorRegistry, currRate);
            }
          } else if (queue < policyConf.bpQueueLowerBound) {
            if (avgCpuUse.getMean() < policyConf.bpIncreaseLowerCpu) {
              // TODO: when to stop increasing rate?
              if (currSourceEvent > aggInput.get() * 0.9 &&
                currRate > avgInputRate.getMean() || currSourceEvent == 0) {
                // This means that we fully consume the event. Stop increasing rate
              } else {
                // Increase rate
                if (avgCpuUse.getMean() < 0.5) {
                  currRate *= policyConf.bpIncreaseRatio;
                  LOG.info("Increase backpressure rate to {}", currRate);
                  sendBackpressure(executorRegistry, currRate);
                } else if (avgCpuUse.getMean() < 0.7) {
                   currRate *= policyConf.bpIncreaseRatio * 0.9;
                  LOG.info("Increase backpressure rate to {}", currRate);
                  sendBackpressure(executorRegistry, currRate);
                }
              }
            }
          }
        }
      } catch (final Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }, Util.THROTTLE_WINDOW, Util.THROTTLE_WINDOW, TimeUnit.MILLISECONDS);
  }

  @Override
  public synchronized void addSourceEvent(final long sourceEvent) {
    currSourceEvent = sourceEvent;
  }

  @Override
  public synchronized void addCurrentInput(final long rate) {
    // Observed that the actual event is the half
    avgInputRate.addValue(rate / 2);
    aggInput.getAndAdd(rate / 2);
  }

  public synchronized void setCurrInput(final long rate) {
    currEmitInput = rate;
  }

}
