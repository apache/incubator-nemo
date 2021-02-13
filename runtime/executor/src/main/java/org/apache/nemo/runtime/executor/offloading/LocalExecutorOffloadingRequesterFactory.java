package org.apache.nemo.runtime.executor.offloading;

import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.offloading.client.OffloadingEventHandler;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

public final class LocalExecutorOffloadingRequesterFactory implements OffloadingRequesterFactory {

  private final double cpulimit;

  @Inject
  private LocalExecutorOffloadingRequesterFactory(
    @Parameter(EvalConf.OffloadingCpuLimit.class) final double cpulimit) {
    this.cpulimit = cpulimit;
  }

  @Override
  public OffloadingRequester getInstance(OffloadingEventHandler eventHandler, String address, int port) {
    return new LocalExecutorOffloadingRequester(address, port, cpulimit);
  }
}
