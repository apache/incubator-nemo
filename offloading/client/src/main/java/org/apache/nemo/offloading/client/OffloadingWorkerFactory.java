package org.apache.nemo.offloading.client;

import io.netty.buffer.ByteBuf;
import org.apache.nemo.offloading.common.OffloadingSerializer;

public interface OffloadingWorkerFactory {

  OffloadingWorker createOffloadingWorker(ByteBuf workerInitBuf,
                                          OffloadingSerializer offloadingSerializer);

  void deleteOffloadingWorker(OffloadingWorker worker);

  int getAndIncreaseDataId();
}
