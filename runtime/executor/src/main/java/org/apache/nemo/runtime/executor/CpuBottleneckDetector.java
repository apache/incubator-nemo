package org.apache.nemo.runtime.executor;

import org.apache.nemo.common.Pair;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public final class CpuBottleneckDetector {
  private static final Logger LOG = LoggerFactory.getLogger(CpuBottleneckDetector.class.getName());

  private final ConcurrentMap<EventHandler<BottleneckEvent>, Boolean> eventHandlers;
  private final ScheduledExecutorService monitorThread;
  private final SystemLoadProfiler profiler;

  @NamedParameter(default_value = "2.0") // sec
  public static final class BottleneckDetectionPeriod implements Name<Double> {
  }

  @NamedParameter(default_value = "2") // sec
  public static final class BottleneckDetectionConsecutive implements Name<Integer> {
  }

  @NamedParameter(default_value = "0.7") // sec
  public static final class BottleneckDetectionCpuThreshold implements Name<Double> {
  }

  private final double r;
  private final int k;
  private final double threshold;
  private int currBottleneckId = 0;

  private int currConsecutive = 0;

  @Inject
  private CpuBottleneckDetector(
    final SystemLoadProfiler profiler,
    @Parameter(BottleneckDetectionPeriod.class) final double r,
    @Parameter(BottleneckDetectionConsecutive.class) final int k,
    @Parameter(BottleneckDetectionCpuThreshold.class) final double threshold) {
    this.r = r;
    this.k = k;
    this.threshold = threshold;
    this.profiler = profiler;
    this.eventHandlers = new ConcurrentHashMap<>();
    this.monitorThread = Executors.newSingleThreadScheduledExecutor();

  }

  public void start() {
    this.monitorThread.scheduleAtFixedRate(() -> {
      final double curCpuLoad = profiler.getCpuLoad();
      LOG.info("Current cpu load: {}, consecutive: {}/{}, threshold: {}",
        curCpuLoad, currConsecutive, k, threshold);

      if (curCpuLoad > threshold) {
        currConsecutive += 1;
      } else {
        if (currConsecutive >= k) {
         final BottleneckEvent event =
          new BottleneckEvent(currBottleneckId,
            curCpuLoad, BottleneckEvent.Type.END);
        eventHandlers.keySet().forEach((eventHandler) -> {
          eventHandler.onNext(event);
        });
        }
        currConsecutive = 0;
      }

      if (currConsecutive == k) {
        // bottleneck!
        currBottleneckId++;
        final BottleneckEvent event =
          new BottleneckEvent(currBottleneckId,
            curCpuLoad, BottleneckEvent.Type.START);
        eventHandlers.keySet().forEach((eventHandler) -> {
          eventHandler.onNext(event);
        });
      }
    }, (long) (r * 1000), (long) (r * 1000), TimeUnit.MILLISECONDS);
  }

  public void setBottleneckHandler(final EventHandler<BottleneckEvent> bottleneckHandler) {
    eventHandlers.put(bottleneckHandler, true);
  }


  public void close() {
    monitorThread.shutdown();
  }

  public static final class BottleneckEvent {
    public enum Type {
      START,
      END
    }

    public final int id;
    public final long startTime;
    public final double cpuLoad;
    public final Type type;

    public BottleneckEvent(final int id,
                           final double cpuLoad,
                           final Type type) {
      this.id = id;
      this.cpuLoad = cpuLoad;
      this.type = type;
      this.startTime = System.currentTimeMillis();
    }

    @Override
    public String toString() {
      return "[BottleneckEvent: " + type.name() + ", " + "id: " + id + ", st: " + startTime + ", load: " + cpuLoad  + "]";
    }
  }

}
