package org.apache.nemo.runtime.lambdaexecutor.middle;

import org.apache.nemo.common.Pair;

import java.util.List;

public final class MiddleOffloadingPrepEvent {
  // key: srcId, destId, value: data
  public final int taskIndex;

  public MiddleOffloadingPrepEvent(final int taskIndex) {
    this.taskIndex = taskIndex;
  }
}
