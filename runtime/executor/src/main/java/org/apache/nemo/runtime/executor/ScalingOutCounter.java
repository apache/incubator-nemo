package org.apache.nemo.runtime.executor;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicInteger;

public final class ScalingOutCounter {

  public final AtomicInteger counter = new AtomicInteger();

  @Inject
  private ScalingOutCounter() {

  }
}
