package org.apache.nemo.runtime.lambdaexecutor;

import java.util.List;


public final class OffloadingResultEvent {
  // nextVertexIds, edgeId, data
  public final List<Triple<List<String>, String, Object>> data;

  public OffloadingResultEvent(final List<Triple<List<String>, String, Object>> data) {
    this.data = data;
  }
}
