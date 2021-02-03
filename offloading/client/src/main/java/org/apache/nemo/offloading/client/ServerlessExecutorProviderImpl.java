package org.apache.nemo.offloading.client;

import org.apache.nemo.offloading.common.*;

import javax.inject.Inject;

public final class ServerlessExecutorProviderImpl implements ServerlessExecutorProvider {

  private final DeprecatedOffloadingWorkerFactory workerFactory;

  @Inject
  private ServerlessExecutorProviderImpl(
    final DeprecatedOffloadingWorkerFactory workerFactory) {
    this.workerFactory = workerFactory;
  }

  @Override
  public <I, O> ServerlessExecutorService<I, O> newCachedPool(OffloadingTransform offloadingTransform,
                                                           OffloadingSerializer<I, O> offloadingSerializer,
                                                           EventHandler<O> eventHandler) {
    return new CachedPoolServerlessExecutorService(
      workerFactory, offloadingTransform, offloadingSerializer, eventHandler);
  }
}
