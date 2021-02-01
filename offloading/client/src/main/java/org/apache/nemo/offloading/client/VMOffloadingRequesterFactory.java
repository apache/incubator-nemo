package org.apache.nemo.offloading.client;

import javax.inject.Inject;

public final class VMOffloadingRequesterFactory implements OffloadingRequesterFactory {

  @Inject
  private VMOffloadingRequesterFactory() {

  }

  @Override
  public OffloadingRequester getInstance(OffloadingEventHandler eventHandler, String address, int port) {
    return new VMOffloadingRequester(eventHandler, address,
      port);
  }
}
