package org.apache.nemo.common.eventhandler;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.Triple;

import java.util.List;


public final class OffloadingResultEvent {
  // vertexId, edgeId, data
  public final List<Triple<String, String, Object>> data;

  public OffloadingResultEvent(final List<Triple<String, String, Object>> data) {
    this.data = data;
  }
}
