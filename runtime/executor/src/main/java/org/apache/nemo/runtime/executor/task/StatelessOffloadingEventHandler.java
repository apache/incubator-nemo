package org.apache.nemo.runtime.executor.task;

import org.apache.nemo.runtime.lambdaexecutor.OffloadingHeartbeatEvent;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingResultEvent;
import org.apache.nemo.offloading.common.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

public final class StatelessOffloadingEventHandler implements EventHandler<Object> {
  private static final Logger LOG = LoggerFactory.getLogger(StatelessOffloadingEventHandler.class.getName());

  private final ConcurrentLinkedQueue<Object> offloadingQueue;
  private final ConcurrentMap<Integer, Long> taskTimeMap;

  public StatelessOffloadingEventHandler(
    final ConcurrentLinkedQueue<Object> offloadingQueue,
    final ConcurrentMap<Integer, Long> taskTimeMap) {
    this.offloadingQueue = offloadingQueue;
    this.taskTimeMap = taskTimeMap;
  }

  @Override
  public void onNext(Object msg) {
    if (msg instanceof OffloadingHeartbeatEvent) {
      final OffloadingHeartbeatEvent event = (OffloadingHeartbeatEvent) msg;
      taskTimeMap.put(event.taskIndex, event.time);

    } else if (msg instanceof OffloadingResultEvent) {
      if (((OffloadingResultEvent) msg).data.size() > 0) {
        //LOG.info("Result received: cnt {}", ((OffloadingResultEvent) msg).data.size());
        offloadingQueue.add(msg);
      }
    } else {
      offloadingQueue.add(msg);
    }
  }
}
