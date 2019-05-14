package org.apache.nemo.runtime.executor.datatransfer;

import org.apache.nemo.common.Pair;
import org.apache.nemo.runtime.executor.common.datatransfer.ByteInputContext;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class TaskTransferIndexMap {
  // key: (runtimeEdgeId, dstTaskIndex), value: input contexts of this task
  private final ConcurrentMap<Pair<String, Pair<Integer, Boolean>>, Integer> map;

  @Inject
  private TaskTransferIndexMap() {
    this.map = new ConcurrentHashMap<>();
  }

  public ConcurrentMap<Pair<String, Pair<Integer, Boolean>>, Integer> getMap() {
    return map;
  }
}
