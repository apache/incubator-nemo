package org.apache.nemo.runtime.master.backpressure;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.nemo.common.Util;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.conf.PolicyConf;
import org.apache.nemo.runtime.master.metric.ExecutorMetricInfo;
import org.apache.nemo.runtime.master.scaler.ExecutorMetricMap;
import org.apache.nemo.runtime.master.scheduler.ExecutorRegistry;
import org.apache.reef.tang.annotations.Parameter;
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

  private long backpressureRate = Long.MAX_VALUE;

  private final DescriptiveStatistics avgCpuUse;
  private final DescriptiveStatistics avgMaxCpuUse;
  private final DescriptiveStatistics avgInputRate;
  private final DescriptiveStatistics avgProcessingRate;
  private final DescriptiveStatistics avgQueueSize;
  private long currSourceEvent = 0;
  private long prevProcessingEvent = 0;

  private final int windowSize = 5;
  private final int sourceParallelism;

  private long scalingHintSetTime = System.currentTimeMillis();

  @Inject
  private InputAndQueueSizeBasedBackpressure(final ExecutorMetricMap executorMetricMap,
                                             final ExecutorRegistry executorRegistry,
                                             @Parameter(EvalConf.SourceParallelism.class) final int sourceParallelism,
                                             final PolicyConf policyConf) {
    this.executorMetricMap = executorMetricMap;
    this.policyConf = policyConf;
    this.executorRegistry = executorRegistry;
    this.sourceParallelism = sourceParallelism;
    this.avgCpuUse = new DescriptiveStatistics(windowSize);
    this.avgMaxCpuUse = new DescriptiveStatistics(windowSize);
    this.avgInputRate = new DescriptiveStatistics(windowSize);
    this.avgProcessingRate = new DescriptiveStatistics(windowSize);
    this.avgQueueSize = new DescriptiveStatistics(windowSize);
    this.backpressureRate = policyConf.bpMinEvent;

    scheduledExecutorService.scheduleAtFixedRate(() -> {
      try {
        LOG.info("Current backpressure rate {}", backpressureRate);

        final ExecutorMetricInfo info = executorMetricMap.getAggregated();

        // Calculate queue size
        final long queue = info.receiveEvent - info.processEvent;
        avgQueueSize.addValue(queue);

        // Update processing rate
        avgProcessingRate.addValue(info.processEvent - prevProcessingEvent);
        prevProcessingEvent = info.processEvent;

        if (info.numExecutor > 0) {
          avgCpuUse.addValue(info.cpuUse / info.numExecutor);
          avgMaxCpuUse.addValue(info.maxCpuUse);
        }

        synchronized (this) {
          LOG.info("Total queue: {}, avg queue: {}, avg cpu: {}, backpressureRate: {}, avgInputRate: {}," +
              "aggInput: {}, sourceEvent: {}, processingRate: {}, maxCPU: {} numExecutor: {}",
            queue, avgQueueSize.getMean(),
            avgCpuUse.getMean(), backpressureRate, avgInputRate.getMean(),
            aggInput, currSourceEvent, avgProcessingRate.getMean(),
            info.maxCpuUse, info.numExecutor);

          cpuBasedBackpressure();
        }
      } catch (final Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }, Util.THROTTLE_WINDOW, Util.THROTTLE_WINDOW, TimeUnit.MILLISECONDS);
  }

  private void cpuBasedBackpressure() {
    // final double avgCpu = avgCpuUse.getMean();
    final double avgCpu = avgMaxCpuUse.getMean();
    if (currSourceEvent == 0) {
      return;
    }

    if (avgCpu < policyConf.bpDecreaseTriggerCpu
      && backpressureRate > avgInputRate.getMean() && avgInputRate.getMean() > 0
      && currSourceEvent > 0) {
      backpressureRate = Math.max(1000, (long) (avgInputRate.getMean() * 1.2));

      LOG.info("Set backpressure rate to {}", backpressureRate);
      sendBackpressureWrapper();
    }

    if (avgCpu > policyConf.bpDecreaseTriggerCpu) {
      // Back pressure
      if (currSourceEvent > aggInput.get() * 0.9 || backpressureRate > avgInputRate.getMean()) {
        final double decreaseRatio = Math.min(1, policyConf.bpDecreaseTriggerCpu / avgCpu);
        backpressureRate = Math.max(1000, (long) (backpressureRate * decreaseRatio));
        LOG.info("Decrease backpressure rate to {}, ratio: {}",
          backpressureRate, policyConf.bpDecreaseTriggerCpu / avgCpu);
        sendBackpressureWrapper();
      }

    } else if (avgCpu < policyConf.bpIncreaseLowerCpu) {
      // TODO: when to stop increasing rate?
      if (currSourceEvent > aggInput.get() * 0.9 &&
        backpressureRate > avgInputRate.getMean() || currSourceEvent == 0) {
        // This means that we fully consume the event. Stop increasing rate
      } else {
        // Increase rate
        final double increaseRatio = Math.max(1, (policyConf.bpDecreaseTriggerCpu / avgCpu) * 0.9);
        backpressureRate *= increaseRatio;
        LOG.info("Increase backpressure rate to {} with ratio {}", backpressureRate, increaseRatio);
        sendBackpressureWrapper();

        /*
        if (avgCpuUse.getMean() < 0.5) {
          backpressureRate *= policyConf.bpIncreaseRatio;
          LOG.info("Increase backpressure rate to {} with ratio {}", backpressureRate, increaseRatio);
          sendBackpressure(executorRegistry, backpressureRate);
        } else if (avgCpuUse.getMean() < 0.6) {
          backpressureRate = Math.max(backpressureRate, (long) (backpressureRate * policyConf.bpIncreaseRatio * 0.9));
          LOG.info("Increase backpressure rate to {}", backpressureRate);
          sendBackpressure(executorRegistry, backpressureRate);
        } else {
          backpressureRate = Math.max(backpressureRate, (long) (backpressureRate * policyConf.bpIncreaseRatio * 0.8));
          LOG.info("Increase backpressure rate to {}", backpressureRate);
          sendBackpressure(executorRegistry, backpressureRate);
        }
        */
      }
    }
  }

  private void sendBackpressureWrapper() {
    if (System.currentTimeMillis() - scalingHintSetTime >= 5000) {
      sendBackpressure(executorRegistry, backpressureRate, sourceParallelism);
    } else {
      LOG.info("Skip setting backpressure due to the scaling hint set time");
    }
  }

  private void queueBasedBackpressure(final long queue) {
    if (queue > policyConf.bpQueueUpperBound) {
      // Back pressure
      if (queue > avgQueueSize.getMean()) {
        if (backpressureRate > avgInputRate.getMean() && avgInputRate.getMean() > 1000) {
          backpressureRate = (long) (avgInputRate.getMean() * policyConf.bpDecreaseRatio);
        } else {
          backpressureRate *= policyConf.bpDecreaseRatio;
        }

        LOG.info("Decrease backpressure rate to {}", backpressureRate);

        sendBackpressure(executorRegistry, backpressureRate, sourceParallelism);
      }
    } else if (queue < policyConf.bpQueueLowerBound) {
      if (avgCpuUse.getMean() < policyConf.bpIncreaseLowerCpu) {
        // TODO: when to stop increasing rate?
        if (currSourceEvent > aggInput.get() * 0.9 &&
          backpressureRate > avgInputRate.getMean() || currSourceEvent == 0) {
          // This means that we fully consume the event. Stop increasing rate
        } else {
          // Increase rate
          if (avgCpuUse.getMean() < 0.5) {
            backpressureRate *= policyConf.bpIncreaseRatio;
            LOG.info("Increase backpressure rate to {}", backpressureRate);
            sendBackpressure(executorRegistry, backpressureRate, sourceParallelism);
          } else if (avgCpuUse.getMean() < 0.7) {
            backpressureRate *= policyConf.bpIncreaseRatio * 0.9;
            LOG.info("Increase backpressure rate to {}", backpressureRate);
            sendBackpressure(executorRegistry, backpressureRate, sourceParallelism);
          } else {
            backpressureRate = Math.max(backpressureRate, (long) (backpressureRate * policyConf.bpIncreaseRatio * 0.8));
            LOG.info("Increase backpressure rate to {}", backpressureRate);
            sendBackpressure(executorRegistry, backpressureRate, sourceParallelism);
          }
        }
      }
    }
  }

  @Override
  public synchronized void addSourceEvent(final long sourceEvent) {
    currSourceEvent = sourceEvent;
  }

  @Override
  public synchronized void addCurrentInput(final long rate) {
    // Observed that the actual event is the half
    avgInputRate.addValue(rate);
    aggInput.getAndAdd(rate);
  }

  @Override
  public synchronized void setHintForScaling(double scalingRatio) {
    final long prevRate = backpressureRate;
    backpressureRate = Math.max(backpressureRate,  (long)((backpressureRate / (1 - scalingRatio)) * 1.1));

    LOG.info("Set hint for scaling {} and increase backpressure rate to {}, prev {}", scalingRatio, backpressureRate, prevRate);

    if (backpressureRate > prevRate) {
      scalingHintSetTime = System.currentTimeMillis();
      sendBackpressure(executorRegistry, backpressureRate, sourceParallelism);
    }
  }
}
