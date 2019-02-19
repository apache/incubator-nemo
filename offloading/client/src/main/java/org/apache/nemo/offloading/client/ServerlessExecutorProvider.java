package org.apache.nemo.offloading.client;

import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.offloading.common.OffloadingSerializer;
import org.apache.nemo.offloading.common.OffloadingTransform;

public interface ServerlessExecutorProvider {

  <I, O> ServerlessExecutorService<I> newCachedPool(
    OffloadingTransform offloadingTransform,
    OffloadingSerializer<I, O> offloadingSerializer,
    EventHandler<O> eventHandler);
}
