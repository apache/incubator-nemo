package org.apache.nemo.runtime.executor.monitoring;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.nemo.common.Pair;
import org.apache.nemo.runtime.executor.monitoring.CpuEventModel;

import javax.inject.Inject;
import java.util.ArrayDeque;
import java.util.Queue;

public final class LinearCpuEventModel implements CpuEventModel<Integer> {

  private final SimpleRegression regression;

  private final int length = 300;
  private final Queue<Pair<Double, Integer>> data = new ArrayDeque<>(length);

  @Inject
  private LinearCpuEventModel() {
    this.regression = new SimpleRegression();
  }

  public synchronized void add(final double cpuLoad,
                  final Integer processedCnt) {

    if (data.size() == length) {
      final Pair<Double, Integer> event = data.poll();
      regression.removeData(event.left(), event.right());
    }

    data.add(Pair.of(cpuLoad, processedCnt));
    regression.addData(cpuLoad, processedCnt);
  }

  public synchronized Integer desirableMetricForLoad(final double targetLoad) {
    return (int) regression.predict(targetLoad);
  }
}
