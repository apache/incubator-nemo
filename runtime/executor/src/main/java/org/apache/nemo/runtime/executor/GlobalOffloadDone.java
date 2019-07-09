package org.apache.nemo.runtime.executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public final class GlobalOffloadDone {

  private static final Logger LOG = LoggerFactory.getLogger(GlobalOffloadDone.class);

  private final AtomicBoolean done = new AtomicBoolean(true);

  private static final GlobalOffloadDone INSTANCE = new GlobalOffloadDone();

  public static GlobalOffloadDone getInstance() {
    return INSTANCE;
  }

  public AtomicBoolean getBoolean() {
    return done;
  }
}
