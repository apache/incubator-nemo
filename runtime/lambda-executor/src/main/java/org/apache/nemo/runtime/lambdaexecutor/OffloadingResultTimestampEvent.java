package org.apache.nemo.runtime.lambdaexecutor;

import java.util.List;


public final class OffloadingResultTimestampEvent {
  // nextVertexIds, edgeId, data
  public final String taskId;
  public final String vertexId;
  public final long watermark;
  public final long timestamp;

  public OffloadingResultTimestampEvent(final String taskId,
                                        final String vertexId,
                                        final long timestamp,
                                        final long watermark) {
    this.taskId = taskId;
    this.vertexId = vertexId;
    this.timestamp = timestamp;
    this.watermark = watermark;
  }

  @Override
  public String toString() {
    return "vertex: " + vertexId + ", timestamp: " + timestamp;
  }
}
