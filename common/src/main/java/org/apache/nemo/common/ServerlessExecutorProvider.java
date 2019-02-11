package org.apache.nemo.common;

import org.apache.nemo.common.EventHandler;
import org.apache.nemo.common.OffloadingSerializer;
import org.apache.nemo.common.OffloadingTransform;

public interface ServerlessExecutorProvider {

  <I, O> ServerlessExecutorService<I> newCachedPool(
    OffloadingTransform offloadingTransform,
    OffloadingSerializer<I, O> offloadingSerializer,
    EventHandler<O> eventHandler);
}
