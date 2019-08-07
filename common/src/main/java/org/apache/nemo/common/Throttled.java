package org.apache.nemo.common;

import java.util.concurrent.atomic.AtomicBoolean;

public final class Throttled {

  private static final Throttled INSTANCE = new Throttled();

  private final AtomicBoolean throttled = new AtomicBoolean(false);

  private Throttled() {

  }

  public void setThrottle(final boolean b) {
    throttled.set(b);
  }

  public boolean getThrottled() {
    return throttled.get();
  }

  public static Throttled getInstance() {
    return INSTANCE;
  }
}
