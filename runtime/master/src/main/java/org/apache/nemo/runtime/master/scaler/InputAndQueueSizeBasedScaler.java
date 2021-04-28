package org.apache.nemo.runtime.master.scaler;

import org.apache.nemo.common.Util;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.runtime.master.metric.ExecutorMetricInfo;

import javax.inject.Inject;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class InputAndQueueSizeBasedScaler {

  private AtomicLong aggInput = new AtomicLong(0);
  private long currEmitInput = 0;
  private final ScheduledExecutorService scheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor();

  private final ExecutorMetricMap executorMetricMap;
  private final EvalConf evalConf;

  @Inject
  private InputAndQueueSizeBasedScaler(final ExecutorMetricMap executorMetricMap,
                                       final EvalConf evalConf) {
    this.executorMetricMap = executorMetricMap;
    this.evalConf = evalConf;

    scheduledExecutorService.scheduleAtFixedRate(() -> {

      // Calculate queue size
      final ExecutorMetricInfo info = executorMetricMap.getAggregated();
      final long queue = info.receiveEvent - info.processEvent;

      if (queue > evalConf.bpQueueSize) {
        // Back pressure

      } else if (queue < 2000) {
        // Increase input rate

        // 1. current input rate
        // 2.
      }

    }, Util.THROTTLE_WINDOW, Util.THROTTLE_WINDOW, TimeUnit.MILLISECONDS);

  }


  public synchronized void addCurrentInput(final long rate) {
    aggInput.getAndAdd(rate);
  }

  public synchronized void setCurrInput(final long rate) {
    currEmitInput = rate;
  }



}
