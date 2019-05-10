package org.apache.nemo.runtime.executor.common.datatransfer;

import javax.inject.Inject;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public final class AckScheduledService {

  public final ScheduledExecutorService ackService;

  @Inject
  public AckScheduledService() {
    this.ackService = Executors.newScheduledThreadPool(5);
  }
}
