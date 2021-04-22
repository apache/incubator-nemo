package org.apache.nemo.runtime.master;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicInteger;

public final class ResourceRequestCounter {
  public final AtomicInteger resourceRequestCount = new AtomicInteger();

  @Inject
  private ResourceRequestCounter() {
  }
}
