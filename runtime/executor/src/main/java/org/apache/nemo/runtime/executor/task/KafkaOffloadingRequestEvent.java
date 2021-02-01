package org.apache.nemo.runtime.executor.task;

import org.apache.nemo.offloading.common.OffloadingWorker;
import org.apache.nemo.runtime.common.comm.ControlMessage;

import java.util.concurrent.CompletableFuture;

public final class KafkaOffloadingRequestEvent {
  public final OffloadingWorker offloadingWorker;
  public final int id;
  public final CompletableFuture<ControlMessage.Message> taskIndexFuture;

  public KafkaOffloadingRequestEvent(final OffloadingWorker offloadingWorker,
                                     final int id,
                                     final CompletableFuture<ControlMessage.Message> taskIndexFuture) {
    this.offloadingWorker = offloadingWorker;
    this.id = id;
    this.taskIndexFuture = taskIndexFuture;
  }
}
