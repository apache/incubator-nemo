package org.apache.nemo.runtime.executor.offloading;

import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.offloading.client.OffloadingEventHandler;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

public final class LocalExecutorOffloadingRequesterFactory implements OffloadingRequesterFactory {

  private final long throttleRate;

  @Inject
  private LocalExecutorOffloadingRequesterFactory(
    @Parameter(EvalConf.ThrottleRate.class) final long throttleRate) {
    this.throttleRate = throttleRate;
  }

  @Override
  public OffloadingRequester getInstance(OffloadingEventHandler eventHandler, String address, int port) {
    return new LocalExecutorOffloadingRequester(address, port, throttleRate);
  }
}
