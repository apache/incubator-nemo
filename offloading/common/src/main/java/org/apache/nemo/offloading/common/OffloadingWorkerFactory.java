package org.apache.nemo.offloading.common;

import io.netty.buffer.ByteBuf;
import org.apache.nemo.offloading.common.OffloadingSerializer;

public interface OffloadingWorkerFactory {

  OffloadingWorker createOffloadingWorker(ByteBuf workerInitBuf,
                                          OffloadingSerializer offloadingSerializer);

  <O> OffloadingWorker createStreamingWorker(ByteBuf workerInitBuf,
                                          OffloadingSerializer offloadingSerializer,
                                         EventHandler<O> eventHandler);

  void deleteOffloadingWorker(OffloadingWorker worker);

  int getAndIncreaseDataId();
}
