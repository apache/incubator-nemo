package org.apache.nemo.compiler.frontend.beam;

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

import java.util.concurrent.*;

public final class WindowAccessMap {

  public static final ConcurrentMap<BoundedWindow, Long> MAP;


  static {
    final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    MAP = new ConcurrentHashMap<>();

    scheduledExecutorService.scheduleAtFixedRate(() -> {
      MAP.forEach((window, accessTime) -> {
        //System.out.println(window + ", " + "final access time: " + accessTime);
      });
    }, 5, 5, TimeUnit.SECONDS);
  }
}
