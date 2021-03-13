package org.apache.nemo.runtime.executor.common.monitoring;

import javax.inject.Inject;

public final class RecentEventbasedCpuEventModel implements CpuEventModel<Integer> {

  private double recentCpuLoad;
  private int recentProcessedCnt;

  @Inject
  private RecentEventbasedCpuEventModel() {
  }


  @Override
  public void add(double cpuLoad, Integer processedCnt) {
    recentCpuLoad = cpuLoad;
    recentProcessedCnt = processedCnt;
  }

  public synchronized Integer desirableMetricForLoad(final double targetLoad) {
    final double slope = recentProcessedCnt / recentCpuLoad;
    return (int) (targetLoad * slope);
  }
}
