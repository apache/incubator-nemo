package org.apache.nemo.runtime.lambdaexecutor.downstream;

import org.apache.nemo.common.Pair;

import java.util.List;

public final class DownstreamOffloadingDataEvent {
  // key: srcId, destId, value: data

  public final Object data;
  public final String nextOpId;

  public DownstreamOffloadingDataEvent(final Object data,
                                       final String opId) {
    this.data = data;
    this.nextOpId = opId;
  }
}
