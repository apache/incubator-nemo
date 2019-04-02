package org.apache.nemo.runtime.executor;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.nemo.common.Pair;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.runtime.executor.task.EndOffloadingKafkaEvent;
import org.apache.nemo.runtime.executor.task.TaskExecutor;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;

public final class TaskOffloader {
  private static final Logger LOG = LoggerFactory.getLogger(TaskOffloader.class.getName());

  private final ScheduledExecutorService monitorThread;
  private final SystemLoadProfiler profiler;

  private final long r;
  private final int k;
  private final double threshold;
  private int currConsecutive = 0;

  private final TaskEventRateCalculator taskEventRateCalculator;
  private final CpuEventModel cpuEventModel;

  // key: offloaded task executor, value: start time of offloading
  private final Queue<Pair<TaskExecutor, Long>> offloadedExecutors;
  private final ConcurrentMap<TaskExecutor, Boolean> taskExecutorMap;
  private long prevDecisionTime = System.currentTimeMillis();
  private long slackTime = 5000;


  private final int windowSize = 2;
  private final DescriptiveStatistics cpuAverage;
  private final DescriptiveStatistics eventAverage;
  private final EvalConf evalConf;

  // DEBUGGIGN
  final ScheduledExecutorService se = Executors.newSingleThreadScheduledExecutor();

  // TODO: high threshold
  // TODO: low threshold ==> threshold 2개 놓기

  @Inject
  private TaskOffloader(
    final SystemLoadProfiler profiler,
    @Parameter(EvalConf.BottleneckDetectionPeriod.class) final long r,
    @Parameter(EvalConf.BottleneckDetectionConsecutive.class) final int k,
    @Parameter(EvalConf.BottleneckDetectionCpuThreshold.class) final double threshold,
    final TaskEventRateCalculator taskEventRateCalculator,
    final TaskExecutorMapWrapper taskExecutorMapWrapper,
    final CpuEventModel cpuEventModel,
    final EvalConf evalConf) {
    this.evalConf = evalConf;
    this.r = r;
    this.k = k;
    this.threshold = threshold;
    this.profiler = profiler;
    this.monitorThread = Executors.newSingleThreadScheduledExecutor();
    this.taskEventRateCalculator = taskEventRateCalculator;
    this.cpuAverage = new DescriptiveStatistics();
    cpuAverage.setWindowSize(windowSize);
    this.eventAverage = new DescriptiveStatistics();
    eventAverage.setWindowSize(2);

    this.taskExecutorMap = taskExecutorMapWrapper.taskExecutorMap;
    this.cpuEventModel = cpuEventModel;
    this.offloadedExecutors = new ArrayDeque<>();
  }

  private boolean timeToDecision(final long currTime) {
    if (currTime - prevDecisionTime >= slackTime) {
      prevDecisionTime = currTime;
      return true;
    } else {
      return false;
    }
  }

  private Collection<TaskExecutor> findOffloadableTasks() {
    final Set<TaskExecutor> taskExecutors = new HashSet<>(taskExecutorMap.keySet());
    for (final Pair<TaskExecutor, Long> pair : offloadedExecutors) {
      taskExecutors.remove(pair.left());
    }

    return taskExecutors;
  }

  private int calculateOFfloadedTasks() {
    int cnt = 0;
    for (final Pair<TaskExecutor, Long> offloadExecutor : offloadedExecutors) {
      if (offloadExecutor.left().isOffloaded()) {
        cnt += 1;
      }
    }
    return cnt;
  }

  private int findTasksThatProcessEvents() {
    int cnt = 0;
    for (final TaskExecutor taskExecutor : taskExecutorMap.keySet()) {
      if (taskExecutor.isRunning() || taskExecutor.isOffloadPending()) {
        cnt += 1;
      }
    }
    return cnt;
  }

  final class TaskStatInfo {
    public final int running;
    public final int offload_pending;
    public final int offloaded;
    public final int deoffloaded;
    public TaskStatInfo(final int running, final int offload_pending, final int offloaded, final int deoffloaded) {
      this.running = running;
      this.offload_pending = offload_pending;
      this.offloaded = offloaded;
      this.deoffloaded = deoffloaded;
    }
  }

  private TaskStatInfo measureTaskStatInfo() {
    int running = 0;
    int offpending = 0;
    int offloaded = 0;
    int deoffpending = 0;
     for (final TaskExecutor taskExecutor : taskExecutorMap.keySet()) {
      if (taskExecutor.isRunning()) {
        running += 1;
      } else if (taskExecutor.isOffloadPending()) {
        offpending += 1;
      } else if (taskExecutor.isOffloaded()) {
        offloaded += 1;
      } else if (taskExecutor.isDeoffloadPending()) {
        deoffpending += 1;
      }
    }

    LOG.info("Task running {}, offload_pending: {}, offloaded: {}, deoffload_pending: {}, total: {}",
      running, offpending, offloaded, deoffpending, taskExecutorMap.size());

     return new TaskStatInfo(running, offpending, offloaded, deoffpending);
  }

  public void startDebugging() {
    // For offloading debugging


    se.scheduleAtFixedRate(() -> {
      LOG.info("Start offloading kafka");
      int cnt = 0;
      final int offloadCnt = taskExecutorMap.keySet().size() - evalConf.minVmTask;
      for (final TaskExecutor taskExecutor : taskExecutorMap.keySet()) {
        if (cnt < offloadCnt) {
          offloadedExecutors.add(Pair.of(taskExecutor, System.currentTimeMillis()));
          taskExecutor.startOffloading(System.currentTimeMillis());
        }
        cnt += 1;
      }
    }, 10, 50, TimeUnit.SECONDS);

    se.scheduleAtFixedRate(() -> {
      LOG.info("End offloading kafka");
      while (!offloadedExecutors.isEmpty()) {
        offloadedExecutors.poll().left().endOffloading();
      }
    }, 30, 50, TimeUnit.SECONDS);
  }


  public void start() {
    this.monitorThread.scheduleAtFixedRate(() -> {
      cpuAverage.addValue(profiler.getCpuLoad());
      eventAverage.addValue(taskEventRateCalculator.calculateProcessedEvent());

      final double cpuMean = cpuAverage.getMean();
      final double eventMean = eventAverage.getMean();

      final long currTime = System.currentTimeMillis();

      LOG.info("Current cpu load: {}, # events: {}, consecutive: {}/{}, threshold: {}",
        cpuMean, eventMean, currConsecutive, k, threshold);


      if (cpuMean < 0.94 && cpuMean > 0.03 && eventMean > 100) {
        // prevent bias
        LOG.info("Add model to {} / {}", cpuMean, eventMean);
        cpuEventModel.add(cpuMean, (int) eventMean);
      }

      if (cpuMean > threshold && eventMean > evalConf.eventThreshold) {

        final TaskStatInfo taskStatInfo = measureTaskStatInfo();
        if (taskStatInfo.running > evalConf.minVmTask) {

          // offload if it is bursty state
          // we should offload some task executors
          final int desirableEvents = cpuEventModel.desirableCountForLoad(threshold);
          final double ratio = desirableEvents / eventMean;
          final int numExecutors = Math.min(taskExecutorMap.size(), taskStatInfo.running + taskStatInfo.offload_pending);
          //final int adjustVmCnt = Math.max(evalConf.minVmTask,
          //  Math.min(numExecutors, (int) Math.ceil(ratio * numExecutors)));
          final int adjustVmCnt = (int) ((evalConf.eventThreshold / eventMean) * numExecutors);
          final int offloadingCnt = Math.max(0, numExecutors - adjustVmCnt - taskStatInfo.offload_pending);

          LOG.info("Start desirable events: {} for load {}, total: {}, desirableVm: {}, currVm: {}, " +
              "offloadingCnt: {}, offloadedExecutors: {}",
            desirableEvents, threshold, eventMean, adjustVmCnt, numExecutors,
            offloadingCnt, offloadedExecutors.size());

          int cnt = 0;
          final List<TaskExecutor> taskExecutors = new ArrayList<>(taskExecutorMap.keySet());
          Collections.sort(taskExecutors, new Comparator<TaskExecutor>() {
            @Override
            public int compare(TaskExecutor o1, TaskExecutor o2) {
              return (int)(o1.getPrevOffloadEndTime().get() - o2.getPrevOffloadEndTime().get());
            }
          });
          for (final TaskExecutor taskExecutor : taskExecutorMap.keySet()) {
            if (taskExecutor.isStateless() && taskExecutor.isRunning()) {
              if (offloadingCnt == cnt) {
                break;
              }

              LOG.info("Start offloading of {}", taskExecutor.getId());
              taskExecutor.startOffloading(currTime);
              offloadedExecutors.add(Pair.of(taskExecutor, currTime));
              cnt += 1;
            }
          }
        }
      } else if (cpuMean < threshold && eventMean < evalConf.eventThreshold) {
        if (!offloadedExecutors.isEmpty()) {
          final TaskStatInfo taskStatInfo = measureTaskStatInfo();
          // if there are offloaded executors
          // we should finish the offloading
          final int desirableEvents = cpuEventModel.desirableCountForLoad(threshold);
          final double ratio = desirableEvents / eventMean;
          final int offloadedCnt = calculateOFfloadedTasks();
          final int numExecutors = Math.max(
            Math.min(taskExecutorMap.size(), taskStatInfo.running + taskStatInfo.offload_pending), 1);
          //final int adjustVmCnt = Math.min(taskExecutorMap.size(), (int) Math.ceil(ratio * numExecutors));
          final int adjustVmCnt = (int) ((evalConf.eventThreshold / eventMean) * numExecutors);
          final int deOffloadingCnt = Math.max(0, adjustVmCnt - numExecutors - taskStatInfo.deoffloaded);

          LOG.info("Stop desirable events: {} for load {}, total: {}, desriableVm: {}, currVm: {}, " +
              "deoffloadingCnt: {}, offloadedExecutors: {}",
            desirableEvents, threshold, eventMean, adjustVmCnt, numExecutors, deOffloadingCnt, offloadedExecutors.size());

          int cnt = 0;
          while (!offloadedExecutors.isEmpty() && cnt < deOffloadingCnt) {
            final Pair<TaskExecutor, Long> pair = offloadedExecutors.peek();
            final TaskExecutor taskExecutor = pair.left();
            final Long offloadingTime = pair.right();

            if (currTime - offloadingTime >= slackTime) {
              offloadedExecutors.poll();
              taskExecutor.endOffloading();
              cnt += 1;
            } else {
              break;
            }
          }

          LOG.info("Actual stop offloading: {}", cnt);
        }
      }
    }, r, r, TimeUnit.MILLISECONDS);
  }

  public void close() {
    monitorThread.shutdown();
  }


  // 어느 시점 (baseTime) 을 기준으로 fluctuation 하였는가?
  private boolean isBursty(final long baseTime,
                           final List<Pair<Long, Double>> processedEvents) {
    final List<Double> beforeBaseTime = new ArrayList<>();
    final List<Double> afterBaseTime = new ArrayList<>();

    for (final Pair<Long, Double> pair : processedEvents) {
      if (pair.left() < baseTime) {
        beforeBaseTime.add(pair.right());
      } else {
        afterBaseTime.add(pair.right());
      }
    }

    final double avgEventBeforeBaseTime = beforeBaseTime.stream()
      .reduce(0.0, (x, y) -> x + y) / (Math.max(1, beforeBaseTime.size()));

    final double avgEventAfterBaseTime = afterBaseTime.stream()
      .reduce(0.0, (x, y) -> x + y) / (Math.max(1, afterBaseTime.size()));

    LOG.info("avgEventBeforeBaseTime: {} (size: {}), avgEventAfterBaseTime: {} (size: {}), baseTime: {}",
      avgEventBeforeBaseTime, beforeBaseTime.size(), avgEventAfterBaseTime, afterBaseTime.size(), baseTime);

    processedEvents.clear();

    if (avgEventBeforeBaseTime * 2 < avgEventAfterBaseTime) {
      return true;
    } else {
      return false;
    }
  }
}
