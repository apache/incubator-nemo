package org.apache.nemo.offloading.common;

import io.netty.buffer.ByteBuf;


public interface OffloadingWorkerFactory {

  int getDataTransportPort();

  OffloadingWorker createOffloadingWorker(ByteBuf workerInitBuf,
                                          OffloadingSerializer offloadingSerializer);

  OffloadingWorker createStreamingWorker(ByteBuf workerInitBuf,
                                         OffloadingSerializer offloadingSerializer,
                                         EventHandler eventHandler);

  void deleteOffloadingWorker(OffloadingWorker worker);

  int getAndIncreaseDataId();
}
