package org.apache.nemo.runtime.executor.common.monitoring;


import org.apache.reef.tang.annotations.DefaultImplementation;

@DefaultImplementation(RecentEventbasedCpuEventModel.class)
public interface CpuEventModel<T> {
  void add(final double cpuLoad,
                  final T processedCnt);

  T desirableMetricForLoad(final double targetLoad);
}
