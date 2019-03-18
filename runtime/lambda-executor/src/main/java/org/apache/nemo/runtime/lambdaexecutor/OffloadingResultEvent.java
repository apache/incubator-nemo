package org.apache.nemo.runtime.lambdaexecutor;

import java.util.List;


public final class OffloadingResultEvent {
  // nextVertexIds, edgeId, data
  public final List<Triple<List<String>, String, Object>> data;
  public final long watermark;

  public OffloadingResultEvent(final List<Triple<List<String>, String, Object>> data,
                               final long watermark) {
    this.data = data;
    this.watermark = watermark;
  }
}
