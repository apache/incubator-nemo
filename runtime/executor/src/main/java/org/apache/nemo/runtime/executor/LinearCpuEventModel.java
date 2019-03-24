package org.apache.nemo.runtime.executor;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.nemo.common.Pair;

import javax.inject.Inject;
import java.util.ArrayDeque;
import java.util.Queue;

public final class LinearCpuEventModel implements CpuEventModel {

  private final SimpleRegression regression;

  private final int length = 300;
  private final Queue<Pair<Double, Integer>> data = new ArrayDeque<>(length);

  @Inject
  private LinearCpuEventModel() {
    this.regression = new SimpleRegression();
  }

  public synchronized void add(final double cpuLoad,
                  final int processedCnt) {

    if (data.size() == length) {
      final Pair<Double, Integer> event = data.poll();
      regression.removeData(event.left(), event.right());
    }

    data.add(Pair.of(cpuLoad, processedCnt));
    regression.addData(cpuLoad, processedCnt);
  }

  public synchronized int desirableCountForLoad(final double targetLoad) {
    return (int) regression.predict(targetLoad);
  }
}
