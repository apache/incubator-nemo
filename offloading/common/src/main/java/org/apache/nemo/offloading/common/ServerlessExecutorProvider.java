package org.apache.nemo.offloading.common;

public interface ServerlessExecutorProvider {

  <I, O> ServerlessExecutorService<I> newCachedPool(
    OffloadingTransform offloadingTransform,
    OffloadingSerializer<I, O> offloadingSerializer,
    // output event handler
    EventHandler<O> eventHandler);
}
