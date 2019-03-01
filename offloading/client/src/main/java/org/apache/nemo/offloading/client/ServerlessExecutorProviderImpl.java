package org.apache.nemo.offloading.client;

import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.offloading.common.OffloadingSerializer;
import org.apache.nemo.offloading.common.OffloadingTransform;

import javax.inject.Inject;

public final class ServerlessExecutorProviderImpl implements ServerlessExecutorProvider {

  private final OffloadingWorkerFactory workerFactory;

  @Inject
  private ServerlessExecutorProviderImpl(
    final OffloadingWorkerFactory workerFactory) {
    this.workerFactory = workerFactory;
  }

  @Override
  public <I, O> ServerlessExecutorService<I> newCachedPool(OffloadingTransform offloadingTransform,
                                                              OffloadingSerializer<I, O> offloadingSerializer,
                                                              EventHandler<O> eventHandler) {
    return new CachedPoolServerlessExecutorService(
      workerFactory, offloadingTransform, offloadingSerializer, eventHandler);
  }
}
