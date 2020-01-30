package org.apache.nemo.runtime.executor.datatransfer;

import org.apache.nemo.common.TransferKey;

import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class TaskTransferIndexMap {
  // key: (runtimeEdgeId, dstTaskIndex), value: input contexts of this task
  private final ConcurrentMap<TransferKey, Integer> map;

  @Inject
  private TaskTransferIndexMap() {
    this.map = new ConcurrentHashMap<>();
  }

  public ConcurrentMap<TransferKey, Integer> getMap() {
    return map;
  }
}
