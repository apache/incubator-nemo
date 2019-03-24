package org.apache.nemo.runtime.executor;

import org.apache.commons.math3.fitting.PolynomialCurveFitter;
import org.apache.commons.math3.fitting.WeightedObservedPoints;
import org.apache.nemo.common.Pair;

import javax.inject.Inject;
import java.util.ArrayDeque;
import java.util.Queue;

public final class PolynomialCpuEventModel implements CpuEventModel {


  private final int length = 30;
  private final int poly = 3;
  private final Queue<Pair<Double, Integer>> data = new ArrayDeque<>(length);
  private final WeightedObservedPoints obs = new WeightedObservedPoints();
  final PolynomialCurveFitter fitter = PolynomialCurveFitter.create(poly);

  @Inject
  private PolynomialCpuEventModel() {
  }

  public synchronized void add(final double cpuLoad,
                               final int processedCnt) {

    if (data.size() == length) {
      final Pair<Double, Integer> event = data.poll();
    }

    data.add(Pair.of(cpuLoad, processedCnt));
  }

  public synchronized int desirableCountForLoad(final double targetLoad) {

    for (final Pair<Double, Integer> d : data) {
      obs.add(d.left(), d.right());
    }

    final double[] coef = fitter.fit(obs.toList());
    obs.clear();

    return calculateTargetCount(targetLoad, coef);
  }

  private int calculateTargetCount(final double targetLoad, final double[] coeff) {
    double sum = 0;
    for (int i = 0; i < coeff.length; i++) {
      sum += coeff[i] * Math.pow(targetLoad, i);
    }

    return (int)sum;
  }
}
