package org.apache.nemo.runtime.executor.task;

import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingResultEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;

public final class KafkaOffloadingEventHandler implements EventHandler<OffloadingResultEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaOffloadingEventHandler.class.getName());

  private final ConcurrentLinkedQueue<Object> offloadingQueue;

  public KafkaOffloadingEventHandler(
    final ConcurrentLinkedQueue<Object> offloadingQueue) {
    this.offloadingQueue = offloadingQueue;
  }

  @Override
  public void onNext(OffloadingResultEvent msg) {
    LOG.info("Result received: cnt {}", msg.data.size());
    offloadingQueue.add(msg);
  }
}
