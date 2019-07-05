package org.apache.nemo.runtime.executor.common;

import java.io.Flushable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class OutputWriterFlusher {

  private final long intervalMs;
  private final ScheduledExecutorService scheduledExecutorService;

  private final List<Flushable> flushableList;

  public OutputWriterFlusher(final long intervalMs) {
    this.intervalMs = intervalMs;
    this.scheduledExecutorService = Executors.newScheduledThreadPool(5);
    this.flushableList = new ArrayList<>();

    scheduledExecutorService.scheduleAtFixedRate(() -> {

      synchronized (flushableList) {
        flushableList.forEach(flusable -> {
          try {
            flusable.flush();
          } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
        });
      }

    }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
  }

  public void registerFlushable(final Flushable flushable) {

    synchronized (flushableList) {
      flushableList.add(flushable);
    }
  }

  public void removeFlushable(final Flushable flushable) {

    synchronized (flushableList) {
      flushableList.remove(flushable);
    }
  }
}
