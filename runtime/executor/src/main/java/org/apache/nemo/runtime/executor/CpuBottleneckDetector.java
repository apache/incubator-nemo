package org.apache.nemo.runtime.executor;

import org.apache.nemo.conf.EvalConf;
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

  private final long r;
  private final int k;
  private final double threshold;
  private int currBottleneckId = 0;
  private int currConsecutive = 0;
  private int endConsecutive = 0;

  private final TaskEventRateCalculator taskEventRateCalculator;
  private final CpuEventModel cpuEventModel;

  // TODO: high threshold
  // TODO: low threshold ==> threshold 2개 놓기

  @Inject
  private CpuBottleneckDetector(
    final SystemLoadProfiler profiler,
    @Parameter(EvalConf.BottleneckDetectionPeriod.class) final long r,
    @Parameter(EvalConf.BottleneckDetectionConsecutive.class) final int k,
    @Parameter(EvalConf.BottleneckDetectionCpuThreshold.class) final double threshold,
    final TaskEventRateCalculator taskEventRateCalculator,
    final CpuEventModel cpuEventModel) {
    this.r = r;
    this.k = k;
    this.threshold = threshold;
    this.profiler = profiler;
    this.eventHandlers = new ConcurrentHashMap<>();
    this.monitorThread = Executors.newSingleThreadScheduledExecutor();
    this.taskEventRateCalculator = taskEventRateCalculator;
    this.cpuEventModel = cpuEventModel;
  }

  public void start() {
    this.monitorThread.scheduleAtFixedRate(() -> {
      final double curCpuLoad = profiler.getCpuLoad();
      final int processedEvent = taskEventRateCalculator.calculateProcessedEvent();
      LOG.info("Current cpu load: {}, # events: {}, consecutive: {}/{}, threshold: {}",
        curCpuLoad, processedEvent, currConsecutive, k, threshold);

      if (currConsecutive <= k || curCpuLoad < threshold) {
        // prevent bias
        LOG.info("Add model to {} / {}", curCpuLoad, processedEvent);
        cpuEventModel.add(curCpuLoad, processedEvent);
      }

      if (curCpuLoad > threshold) {
        currConsecutive += 1;

        if (currConsecutive == k) {
          // bottleneck!

          currBottleneckId++;
          final BottleneckEvent event =
            new BottleneckEvent(currBottleneckId,
              System.currentTimeMillis() - (k * r),
              curCpuLoad, BottleneckEvent.Type.START, processedEvent);
          eventHandlers.keySet().forEach((eventHandler) -> {
            eventHandler.onNext(event);
          });
        }
      } else {
        if (currConsecutive >= k) {
          // we already detect the bottleneck
          if (endConsecutive == k) {
            final BottleneckEvent event =
              new BottleneckEvent(currBottleneckId,
                System.currentTimeMillis(),
                curCpuLoad, BottleneckEvent.Type.END, processedEvent);
            eventHandlers.keySet().forEach((eventHandler) -> {
              eventHandler.onNext(event);
            });

            currConsecutive = 0;
            endConsecutive = 0;
          } else {
            endConsecutive += 1;
          }
        } else {
          // didn't detect the bottleneck
          // decrease curr consecutive
          currConsecutive = Math.max(0, currConsecutive - 1);
        }
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
    public final int processedEvents;

    public BottleneckEvent(final int id,
                           final long startTime,
                           final double cpuLoad,
                           final Type type,
                           final int processedEvents) {
      this.id = id;
      this.startTime = startTime;
      this.cpuLoad = cpuLoad;
      this.type = type;
      this.endTime = System.currentTimeMillis();
      this.processedEvents = processedEvents;
    }

    @Override
    public String toString() {
      return "[BottleneckEvent: " + type.name() + ", " + "id: " + id + ", [" + startTime + "-" + endTime + ")" + ", load: " + cpuLoad  + "]";
    }
  }

}
