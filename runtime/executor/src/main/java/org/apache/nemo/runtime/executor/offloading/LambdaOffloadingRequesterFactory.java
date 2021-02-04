package org.apache.nemo.runtime.executor.offloading;

import org.apache.nemo.offloading.client.OffloadingEventHandler;

import javax.inject.Inject;

public final class LambdaOffloadingRequesterFactory implements OffloadingRequesterFactory {

  @Inject
  private LambdaOffloadingRequesterFactory() {

  }

  @Override
  public OffloadingRequester getInstance(OffloadingEventHandler eventHandler, String address, int port) {
    return new LambdaOffloadingRequester(address, port);
  }
}
