package org.apache.nemo.offloading.client;

import io.netty.buffer.ByteBuf;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.offloading.common.OffloadingSerializer;
import org.apache.nemo.offloading.common.OffloadingTransform;

public interface ServerlessExecutorProvider {

  <I, O> ServerlessExecutorService<I> newCachedPool(
    OffloadingTransform offloadingTransform,
    OffloadingSerializer<I, O> offloadingSerializer,
    EventHandler<O> eventHandler);


  <I, O> ServerlessExecutorService<I> newCachedPool(
    ByteBuf buf,
    OffloadingSerializer<I, O> offloadingSerializer,
    EventHandler<O> eventHandler);
}
