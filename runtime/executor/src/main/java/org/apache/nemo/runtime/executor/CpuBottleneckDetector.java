package org.apache.nemo.runtime.executor;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.concurrent.*;

public final class CpuBottleneckDetector {
  private static final Logger LOG = LoggerFactory.getLogger(CpuBottleneckDetector.class.getName());

  private final ConcurrentMap<EventHandler<BottleneckEvent>, Boolean> eventHandlers;
  private final ScheduledExecutorService monitorThread;
  private final SystemLoadProfiler profiler;

  @NamedParameter(default_value = "2000") // msec
  public static final class BottleneckDetectionPeriod implements Name<Long> {
  }

  @NamedParameter(default_value = "2")
  public static final class BottleneckDetectionConsecutive implements Name<Integer> {
  }

  @NamedParameter(default_value = "0.7")
  public static final class BottleneckDetectionCpuThreshold implements Name<Double> {
  }

  private final long r;
  private final int k;
  private final double threshold;
  private int currBottleneckId = 0;
  private int currConsecutive = 0;

  @Inject
  private CpuBottleneckDetector(
    final SystemLoadProfiler profiler,
    @Parameter(BottleneckDetectionPeriod.class) final long r,
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
           System.currentTimeMillis(),
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
            System.currentTimeMillis() - (k * r),
            curCpuLoad, BottleneckEvent.Type.START);
        eventHandlers.keySet().forEach((eventHandler) -> {
          eventHandler.onNext(event);
        });
      }
    }, r, r, TimeUnit.MILLISECONDS);
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
    public final long endTime;
    public final long startTime;
    public final double cpuLoad;
    public final Type type;

    public BottleneckEvent(final int id,
                           final long startTime,
                           final double cpuLoad,
                           final Type type) {
      this.id = id;
      this.startTime = startTime;
      this.cpuLoad = cpuLoad;
      this.type = type;
      this.endTime = System.currentTimeMillis();
    }

    @Override
    public String toString() {
      return "[BottleneckEvent: " + type.name() + ", " + "id: " + id + ", [" + startTime + "-" + endTime + ")" + ", load: " + cpuLoad  + "]";
    }
  }

}
