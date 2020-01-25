package org.apache.nemo.offloading.common;

import io.netty.buffer.ByteBuf;
import org.apache.nemo.offloading.common.OffloadingSerializer;

import java.util.Optional;

public interface OffloadingWorkerFactory {

  OffloadingWorker createOffloadingWorker(ByteBuf workerInitBuf,
                                          OffloadingSerializer offloadingSerializer);

  OffloadingWorker createStreamingWorker(ByteBuf vmScalingInfoBuf,
                                             ByteBuf workerInitBuf,
                                          OffloadingSerializer offloadingSerializer,
                                         EventHandler eventHandler);

  void deleteOffloadingWorker(OffloadingWorker worker);

  int getAndIncreaseDataId();
}
