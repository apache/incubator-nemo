package org.apache.nemo.runtime.executor.offloading;

import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.offloading.client.OffloadingEventHandler;

import javax.inject.Inject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class LambdaOffloadingRequesterFactory implements OffloadingRequesterFactory {

  private final EvalConf evalConf;
  private final ExecutorService executorService;

  @Inject
  private LambdaOffloadingRequesterFactory(final EvalConf evalConf) {
    this.evalConf = evalConf;
    this.executorService = Executors.newCachedThreadPool();
  }

  @Override
  public OffloadingRequester getInstance(OffloadingEventHandler eventHandler, String address, int port) {
    return new LambdaOffloadingRequester(address, port, evalConf.awsRegion, executorService);
  }
}
