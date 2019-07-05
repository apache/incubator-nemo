package org.apache.nemo.runtime.executor;

import org.apache.nemo.runtime.executor.relayserver.RelayServerChannelInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public final class RemainingOffloadTasks {

  private static final Logger LOG = LoggerFactory.getLogger(RemainingOffloadTasks.class);

  private final AtomicInteger remaining = new AtomicInteger();

  private static final RemainingOffloadTasks INSTANCE = new RemainingOffloadTasks();

  public static RemainingOffloadTasks getInstance() {
    return INSTANCE;
  }

  public void set(final int cnt) {
    LOG.info("Set offloadCnt to {}", cnt);
    remaining.set(cnt);
  }

  public int getAndIncrement() {
    return remaining.getAndIncrement();
  }

  public int decrementAndGet() {
    final int cnt = remaining.decrementAndGet();
    LOG.info("Decrement and get remaining offload cnt: {}", cnt);
    return cnt;
  }

  public int getRemainingCnt() {
    return remaining.get();
  }
}
