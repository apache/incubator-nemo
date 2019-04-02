package org.apache.nemo.runtime.executor;

import org.apache.commons.math3.fitting.PolynomialCurveFitter;
import org.apache.commons.math3.fitting.WeightedObservedPoints;
import org.apache.nemo.common.Pair;

import javax.inject.Inject;
import java.util.ArrayDeque;
import java.util.Queue;

public final class PolynomialCpuTimeModel implements CpuEventModel<Long> {


  private final int length = 100;
  private final int poly = 2;
  private final Queue<Pair<Double, Long>> data = new ArrayDeque<>(length);
  private final WeightedObservedPoints obs = new WeightedObservedPoints();
  final PolynomialCurveFitter fitter = PolynomialCurveFitter.create(poly);

  @Inject
  private PolynomialCpuTimeModel() {
  }

  public synchronized void add(final double cpuLoad,
                               final Long processedCnt) {

    if (data.size() == length) {
      final Pair<Double, Long> event = data.poll();
    }

    data.add(Pair.of(cpuLoad, processedCnt));
  }

  @Override
  public synchronized Long desirableMetricForLoad(final double targetLoad) {

    for (final Pair<Double, Long> d : data) {
      obs.add(d.left(), d.right());
    }

    final double[] coef = fitter.fit(obs.toList());
    obs.clear();

    return calculateTargetCount(targetLoad, coef);
  }

  private long calculateTargetCount(final double targetLoad, final double[] coeff) {
    double sum = 0;
    for (int i = 0; i < coeff.length; i++) {
      sum += coeff[i] * Math.pow(targetLoad, i);
    }

    return (long)sum;
  }
}
