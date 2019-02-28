package org.apache.nemo.offloading.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.offloading.common.OffloadingSerializer;
import org.apache.nemo.offloading.common.OffloadingTransform;

import javax.inject.Inject;
import java.io.IOException;
import java.io.ObjectOutputStream;

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
    final ByteBuf byteBuf = Unpooled.buffer();
    final ByteBufOutputStream bos = new ByteBufOutputStream(byteBuf);
    final ObjectOutputStream oos;
    try {
      oos = new ObjectOutputStream(bos);
      oos.writeObject(offloadingTransform);
      oos.close();
      bos.close();
      return newCachedPool(byteBuf, offloadingSerializer, eventHandler);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public <I, O> ServerlessExecutorService<I> newCachedPool(ByteBuf buf, OffloadingSerializer<I, O> offloadingSerializer, EventHandler<O> eventHandler) {
    return new CachedPoolServerlessExecutorService(
      workerFactory, buf, offloadingSerializer, eventHandler);
  }
}
