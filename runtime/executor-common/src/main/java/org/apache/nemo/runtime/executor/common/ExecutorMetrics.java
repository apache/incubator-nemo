package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.common.Pair;

import javax.inject.Inject;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public final class ExecutorMetrics implements Serializable {

  public final Map<ExecutorThread, AtomicLong> inputReceiveCntMap = new ConcurrentHashMap<>();
  public final AtomicLong sourceReceiveCnt = new AtomicLong();
  public final Map<ExecutorThread, Long> inputProcessCntMap = new ConcurrentHashMap<>();
  public final Map<ExecutorThread, Long> inputProcessTimeMap = new ConcurrentHashMap<>();
  public double load;
  public long processingRate;

  @Inject
  public ExecutorMetrics() {

  }
}

