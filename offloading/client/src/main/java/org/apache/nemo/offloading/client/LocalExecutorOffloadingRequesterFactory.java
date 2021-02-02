package org.apache.nemo.offloading.client;

import javax.inject.Inject;

public final class LocalExecutorOffloadingRequesterFactory implements OffloadingRequesterFactory {

  @Inject
  private LocalExecutorOffloadingRequesterFactory() {

  }

  @Override
  public OffloadingRequester getInstance(OffloadingEventHandler eventHandler, String address, int port) {
    return new LocalExecutorOffloadingRequester(address, port);
  }
}
