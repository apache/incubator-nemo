package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.function.Function;

public final class ExecutorGlobalInstances implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutorGlobalInstances.class.getName());

  private final ScheduledExecutorService watermarkTriggerService;
  private final ScheduledExecutorService pollingTrigger;
  private static final long WATERMARK_PERIOD = 250; // ms
  private final Queue<Pair<Object, Runnable>> watermarkServices;


  public ExecutorGlobalInstances() {
    this.watermarkTriggerService = Executors.newScheduledThreadPool(10);
    this.watermarkServices = new ConcurrentLinkedQueue<>();
    this.pollingTrigger = Executors.newScheduledThreadPool(3);

    this.watermarkTriggerService.scheduleAtFixedRate(() -> {
        //LOG.info("Trigger watermarks:{}", watermarkServices.size());
        watermarkServices.forEach(pair -> pair.right().run());
    }, WATERMARK_PERIOD, WATERMARK_PERIOD, TimeUnit.MILLISECONDS);
  }

  public ScheduledExecutorService getPollingTrigger() {
    return pollingTrigger;
  }

  public void registerWatermarkService(final Object sv, final Runnable runnable) {
    //LOG.info("Register {}: ", sv);
    watermarkServices.add(Pair.of(sv, runnable));
  }

  public void deregisterWatermarkService(final Object taskId) {
    final Iterator<Pair<Object, Runnable>> iterator = watermarkServices.iterator();
    while (iterator.hasNext()) {
      final Pair<Object, Runnable> pair = iterator.next();
      if (pair.left().equals(taskId)) {
        iterator.remove();
        return;
      }
    }
  }

  @Override
  public void close() throws Exception {
    watermarkTriggerService.shutdown();
  }
}
