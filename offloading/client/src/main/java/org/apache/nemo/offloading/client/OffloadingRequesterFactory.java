package org.apache.nemo.offloading.client;

import org.apache.reef.tang.annotations.DefaultImplementation;

@DefaultImplementation(VMOffloadingRequesterFactory.class)
public interface OffloadingRequesterFactory {

  OffloadingRequester getInstance(OffloadingEventHandler eventHandler,
                                  String address,
                                  int port);
}
