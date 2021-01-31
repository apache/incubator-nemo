package org.apache.nemo.runtime.executor.task;

import org.apache.nemo.offloading.common.OffloadingWorkerDeprec;
import org.apache.nemo.runtime.common.comm.ControlMessage;

import java.util.concurrent.CompletableFuture;

public final class KafkaOffloadingRequestEvent {
  public final OffloadingWorkerDeprec offloadingWorker;
  public final int id;
  public final CompletableFuture<ControlMessage.Message> taskIndexFuture;

  public KafkaOffloadingRequestEvent(final OffloadingWorkerDeprec offloadingWorker,
                                     final int id,
                                     final CompletableFuture<ControlMessage.Message> taskIndexFuture) {
    this.offloadingWorker = offloadingWorker;
    this.id = id;
    this.taskIndexFuture = taskIndexFuture;
  }
}
