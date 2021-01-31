package org.apache.nemo.offloading.common;

import io.netty.buffer.ByteBuf;

public interface OffloadingWorkerFactory {

  OffloadingWorkerDeprec createOffloadingWorker(ByteBuf workerInitBuf,
                                                OffloadingSerializer offloadingSerializer);

  OffloadingWorkerDeprec createStreamingWorker(ByteBuf vmScalingInfoBuf,
                                               ByteBuf workerInitBuf,
                                               OffloadingSerializer offloadingSerializer,
                                               EventHandler eventHandler);

  void deleteOffloadingWorker(OffloadingWorkerDeprec worker);

  int getAndIncreaseDataId();
}
