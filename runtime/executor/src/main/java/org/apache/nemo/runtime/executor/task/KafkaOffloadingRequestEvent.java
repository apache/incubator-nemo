package org.apache.nemo.runtime.executor.task;

import org.apache.nemo.offloading.common.DeprecatedOffloadingWorker;
import org.apache.nemo.runtime.message.comm.ControlMessage;

import java.util.concurrent.CompletableFuture;

public final class KafkaOffloadingRequestEvent {
  public final DeprecatedOffloadingWorker offloadingWorker;
  public final int id;
  public final CompletableFuture<ControlMessage.Message> taskIndexFuture;

  public KafkaOffloadingRequestEvent(final DeprecatedOffloadingWorker offloadingWorker,
                                     final int id,
                                     final CompletableFuture<ControlMessage.Message> taskIndexFuture) {
    this.offloadingWorker = offloadingWorker;
    this.id = id;
    this.taskIndexFuture = taskIndexFuture;
  }
}
