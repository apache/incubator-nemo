package org.apache.nemo.runtime.executor.offloading;

import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.offloading.client.OffloadingEventHandler;

import javax.inject.Inject;

public final class LambdaOffloadingRequesterFactory implements OffloadingRequesterFactory {

  private final EvalConf evalConf;

  @Inject
  private LambdaOffloadingRequesterFactory(final EvalConf evalConf) {
    this.evalConf = evalConf;
  }

  @Override
  public OffloadingRequester getInstance(OffloadingEventHandler eventHandler, String address, int port) {
    return new LambdaOffloadingRequester(address, port, evalConf.awsRegion);
  }
}
