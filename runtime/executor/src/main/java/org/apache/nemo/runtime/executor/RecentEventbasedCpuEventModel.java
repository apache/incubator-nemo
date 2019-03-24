package org.apache.nemo.runtime.executor;

import org.apache.commons.math3.fitting.PolynomialCurveFitter;
import org.apache.commons.math3.fitting.WeightedObservedPoints;
import org.apache.nemo.common.Pair;

import javax.inject.Inject;
import java.util.ArrayDeque;
import java.util.Queue;

public final class RecentEventbasedCpuEventModel implements CpuEventModel {

  private double recentCpuLoad;
  private int recentProcessedCnt;

  @Inject
  private RecentEventbasedCpuEventModel() {
  }

  public synchronized void add(final double cpuLoad,
                               final int processedCnt) {

    recentCpuLoad = cpuLoad;
    recentProcessedCnt = processedCnt;

  }

  public synchronized int desirableCountForLoad(final double targetLoad) {
    final double slope = recentProcessedCnt / recentCpuLoad;
    return (int) (targetLoad * slope);
  }
}
