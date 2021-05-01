package org.apache.nemo.runtime.master.scaler;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.nemo.conf.PolicyConf;
import org.apache.nemo.runtime.master.ScaleInOutManager;
import org.apache.nemo.runtime.master.metric.ExecutorMetricInfo;
import org.apache.nemo.runtime.master.scheduler.ExecutorRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public final class InputAndCpuBasedScaler implements Scaler {
  private static final Logger LOG = LoggerFactory.getLogger(InputAndCpuBasedScaler.class.getName());

  private final AtomicLong aggInput = new AtomicLong(0);
  private long currEmitInput = 0;

  private final ExecutorMetricMap executorMetricMap;
  private final PolicyConf policyConf;

  private final ScheduledExecutorService scheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor();

  private long currRate = Long.MAX_VALUE;

  private final DescriptiveStatistics avgCpuUse;
  private final DescriptiveStatistics avgInputRate;
  private final DescriptiveStatistics avgSrcProcessingRate;
  private final DescriptiveStatistics avgExpectedCpu;
  private long currSourceEvent = 0;
  private final int windowSize = 5;

  private final ScaleInOutManager scaleInOutManager;
  private final ExecutorRegistry executorRegistry;

  @Inject
  private InputAndCpuBasedScaler(final ExecutorMetricMap executorMetricMap,
                                 final ScaleInOutManager scaleInOutManager,
                                 final ExecutorRegistry executorRegistry,
                                 final PolicyConf policyConf) {
    this.executorMetricMap = executorMetricMap;
    this.policyConf = policyConf;
    this.scaleInOutManager = scaleInOutManager;
    this.executorRegistry = executorRegistry;
    this.avgCpuUse = new DescriptiveStatistics(windowSize);
    this.avgInputRate = new DescriptiveStatistics(windowSize);
    this.avgSrcProcessingRate = new DescriptiveStatistics(windowSize);
    this.avgExpectedCpu = new DescriptiveStatistics(windowSize);
    this.currRate = policyConf.bpMinEvent;

    scheduledExecutorService.scheduleAtFixedRate(() -> {
      try {
        final ExecutorMetricInfo info = executorMetricMap.getAggregated();

        if (info.numExecutor > 0) {
          avgCpuUse.addValue(info.cpuUse / info.numExecutor);
        }

        final double avgCpu = avgCpuUse.getMean();
        final double avgProcess = avgSrcProcessingRate.getMean();
        final double avgInput = avgInputRate.getMean();

        if (avgProcess == 0 || info.numExecutor == 0) {
          return;
        }

        if (info.numExecutor > 0) {
          avgExpectedCpu.addValue((avgInput * avgCpu) / avgProcess);
        }


        final double avgExpectedCpuVal = avgExpectedCpu.getMean();

        synchronized (this) {

          LOG.info("Scaler avg cpu: {}, avg expected cpu: {}, target cpu: {}, " +
              "avg input: {}, avg src input: {}, numExecutor: {}",
            avgCpu,
            avgExpectedCpuVal,
            policyConf.scalerTargetCpu,
            avgInput,
            avgProcess,
            info.numExecutor);

          if (avgExpectedCpuVal > policyConf.scalerUpperCpu) {
            // Scale out !!
            // ex) expected cpu val: 2.0, target cpu: 0.6
            // then, we should reduce the current load of cluster down to 0.3 (2.0 * 0.3 = 0.6),
            // which means that we should scale out 70 % of tasks to Lambda (1 - 0.3)
            final double ratioToScaleout = 1 - policyConf.scalerTargetCpu / avgExpectedCpuVal;
            // move ratioToScaleout % of computations to Lambda
            LOG.info("Move {} percent of tasks in all vm executors", ratioToScaleout);

            scaleInOutManager.sendMigrationAllStages(
              ratioToScaleout,
              executorRegistry.getVMComputeExecutors(),
              true);
          }
        }
      } catch (final Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }, 100, 1, TimeUnit.SECONDS);
  }

  @Override
  public synchronized void addSourceEvent(final long sourceEvent) {
    avgSrcProcessingRate.addValue(sourceEvent - currSourceEvent);
    currSourceEvent = sourceEvent;
  }

  @Override
  public synchronized void addCurrentInput(final long rate) {
    // Observed that the actual event is the half
    avgInputRate.addValue(rate / 2);
    aggInput.getAndAdd(rate / 2);
  }
}
