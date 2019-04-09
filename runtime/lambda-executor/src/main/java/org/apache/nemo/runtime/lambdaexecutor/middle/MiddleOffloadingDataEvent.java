package org.apache.nemo.runtime.lambdaexecutor.middle;

import org.apache.nemo.common.Pair;

import java.util.List;

public final class MiddleOffloadingDataEvent {
  // key: srcId, destId, value: data
  public final Pair<List<String>, Object> data;
  public final long watermark;

  public MiddleOffloadingDataEvent(final Pair<List<String>, Object> data,
                                   final long watermark) {
    this.data = data;
    this.watermark = watermark;
  }
}
