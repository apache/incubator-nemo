package org.apache.nemo.runtime.executor.offloading;

import io.netty.buffer.ByteBuf;
import org.apache.nemo.offloading.common.*;
import org.apache.nemo.runtime.executor.task.OffloadingControlEvent;
import org.apache.reef.tang.annotations.DefaultImplementation;


@DefaultImplementation(DefaultOffloadingWorkerFactory.class)
public interface OffloadingWorkerFactory {

  int getDataTransportPort();

  OffloadingWorker createStreamingWorker(OffloadingSerializer offloadingSerializer,
                                         EventHandler<Pair<OffloadingWorker, OffloadingExecutorControlEvent>> eventHandler);

  void deleteOffloadingWorker(OffloadingWorker worker);

  void start();
}
