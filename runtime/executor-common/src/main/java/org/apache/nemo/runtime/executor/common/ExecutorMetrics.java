package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.common.Pair;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public final class ExecutorMetrics implements Serializable {

  public final Map<String, Pair<AtomicLong, AtomicLong>> taskInputProcessRateMap = new ConcurrentHashMap<>();

  @Override
  public String toString() {
    return taskInputProcessRateMap.toString();
  }
}
