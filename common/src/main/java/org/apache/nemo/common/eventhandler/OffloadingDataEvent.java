package org.apache.nemo.common.eventhandler;

import org.apache.nemo.common.Pair;

import java.util.List;

public final class OffloadingDataEvent {
  // key: srcId, value: data
  public final List<Pair<String, Object>> data;

  public OffloadingDataEvent(final List<Pair<String, Object>> data) {
    this.data = data;
  }
}
