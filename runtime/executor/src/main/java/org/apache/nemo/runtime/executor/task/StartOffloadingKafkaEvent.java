package org.apache.nemo.runtime.executor.task;

import org.apache.nemo.runtime.executor.TinyTaskWorker;

import java.util.Optional;

public final class StartOffloadingKafkaEvent {

  public final TinyTaskWorker worker;

  public StartOffloadingKafkaEvent(final TinyTaskWorker worker) {
    this.worker = worker;
  }
}
