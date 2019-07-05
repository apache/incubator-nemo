package org.apache.nemo.runtime.executor;

import java.util.concurrent.atomic.AtomicInteger;

public final class RemainingOffloadTasks {

  private final AtomicInteger remaining = new AtomicInteger();

  private static final RemainingOffloadTasks INSTANCE = new RemainingOffloadTasks();

  public static RemainingOffloadTasks getInstance() {
    return INSTANCE;
  }

  public void set(final int cnt) {
    remaining.set(cnt);
  }

  public int getAndIncrement() {
    return remaining.getAndIncrement();
  }

  public int decrementAndGet() {
    return remaining.decrementAndGet();
  }

  public int getRemainingCnt() {
    return remaining.get();
  }
}
