package org.apache.nemo.runtime.lambdaexecutor;

import org.apache.nemo.common.Pair;

import java.util.List;

public final class OffloadingDataEvent {
  // key: srcId, destId, value: data
  public final List<Pair<List<String>, Object>> data;

  public OffloadingDataEvent(final List<Pair<List<String>, Object>> data) {
    this.data = data;
  }
}
