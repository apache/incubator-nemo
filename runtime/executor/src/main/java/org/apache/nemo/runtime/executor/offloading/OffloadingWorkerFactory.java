package org.apache.nemo.runtime.executor.offloading;

import io.netty.buffer.ByteBuf;
import org.apache.nemo.offloading.common.DeprecatedOffloadingWorker;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.offloading.common.OffloadingSerializer;
import org.apache.reef.tang.annotations.DefaultImplementation;


@DefaultImplementation(DefaultOffloadingWorkerFactory.class)
public interface OffloadingWorkerFactory {

  int getDataTransportPort();

  OffloadingWorker createStreamingWorker(ByteBuf workerInitBuf,
                                                   OffloadingSerializer offloadingSerializer,
                                                   EventHandler eventHandler);

  void deleteOffloadingWorker(OffloadingWorker worker);
}
