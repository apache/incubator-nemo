package org.apache.nemo.runtime.executor.offloading;

import org.apache.nemo.offloading.client.OffloadingEventHandler;
import org.apache.reef.tang.annotations.DefaultImplementation;

@DefaultImplementation(VMOffloadingRequesterFactory.class)
public interface OffloadingRequesterFactory {

  OffloadingRequester getInstance(OffloadingEventHandler eventHandler,
                                  String address,
                                  int port);
}
