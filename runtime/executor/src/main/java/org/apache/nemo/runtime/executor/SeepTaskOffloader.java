package org.apache.nemo.runtime.executor;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.nemo.common.Pair;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.runtime.executor.common.TaskExecutor;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public final class SeepTaskOffloader {
  private static final Logger LOG = LoggerFactory.getLogger(SeepTaskOffloader.class.getName());

  private final ScheduledExecutorService monitorThread;
  private final SystemLoadProfiler profiler;

  private final long r;
  private final int k;
  private final double threshold;
  private int currConsecutive = 0;

  private final TaskEventRateCalculator taskEventRateCalculator;
  private final CpuEventModel cpuEventModel;

  // key: offloaded task executor, value: start time of offloading
  private final List<Pair<TaskExecutor, Long>> offloadedExecutors;
  private final ConcurrentMap<TaskExecutor, Boolean> taskExecutorMap;
  private long prevDecisionTime = System.currentTimeMillis();
  private long slackTime = 10000;
  private long deoffloadSlackTime = 12000;


  private final int windowSize = 5;
  private final DescriptiveStatistics cpuHighAverage;
  private final DescriptiveStatistics cpuLowAverage;
  private final DescriptiveStatistics eventAverage;
  private final EvalConf evalConf;

  // DEBUGGIGN
  final ScheduledExecutorService se = Executors.newSingleThreadScheduledExecutor();

  private Map<TaskExecutor, Long> prevTaskCpuTimeMap = new HashMap<>();
  private int cpuLoadStable = 0;

  // TODO: high threshold
  // TODO: low threshold ==> threshold 2개 놓기

  private final PolynomialCpuTimeModel cpuTimeModel;

  private int observedCnt = 0;
  private final int observeWindow = 5;

  private long prevOffloadingTime = System.currentTimeMillis();

  private final AtomicInteger consecutive = new AtomicInteger(0);

  @Inject
  private SeepTaskOffloader(
    final SystemLoadProfiler profiler,
    @Parameter(EvalConf.BottleneckDetectionPeriod.class) final long r,
    @Parameter(EvalConf.BottleneckDetectionConsecutive.class) final int k,
    @Parameter(EvalConf.BottleneckDetectionCpuThreshold.class) final double threshold,
    final TaskEventRateCalculator taskEventRateCalculator,
    final TaskExecutorMapWrapper taskExecutorMapWrapper,
    final CpuEventModel cpuEventModel,
    final PolynomialCpuTimeModel cpuTimeModel,
    final EvalConf evalConf) {
    this.evalConf = evalConf;
    this.r = r;
    this.k = k;
    this.threshold = threshold;
    this.profiler = profiler;
    this.monitorThread = Executors.newSingleThreadScheduledExecutor();
    this.taskEventRateCalculator = taskEventRateCalculator;
    this.cpuTimeModel = cpuTimeModel;
    this.cpuHighAverage = new DescriptiveStatistics();
    cpuHighAverage.setWindowSize(2);
    this.cpuLowAverage = new DescriptiveStatistics();
    cpuLowAverage.setWindowSize(2);

    this.eventAverage = new DescriptiveStatistics();
    eventAverage.setWindowSize(2);

    this.taskExecutorMap = taskExecutorMapWrapper.getTaskExecutorMap();
    this.cpuEventModel = cpuEventModel;
    this.offloadedExecutors = new LinkedList<>();
  }
//
//
//  private Collection<TaskExecutor> findOffloadableTasks() {
//    final Set<TaskExecutor> taskExecutors = new HashSet<>(taskExecutorMap.keySet());
//    for (final Pair<TaskExecutor, Long> pair : offloadedExecutors) {
//      taskExecutors.remove(pair.left());
//    }
//
//    return taskExecutors;
//  }
//
//  private int calculateOFfloadedTasks() {
//    int cnt = 0;
//    for (final Pair<TaskExecutor, Long> offloadExecutor : offloadedExecutors) {
//      if (offloadExecutor.left().isOffloadedTask()) {
//        cnt += 1;
//      }
//    }
//    return cnt;
//  }
//
//  private int findTasksThatProcessEvents() {
//    int cnt = 0;
//    for (final TaskExecutor taskExecutor : taskExecutorMap.keySet()) {
//      if (taskExecutor.isRunning() || taskExecutor.isOffloadPending()) {
//        cnt += 1;
//      }
//    }
//    return cnt;
//  }
//
//  final class StatelessTaskStatInfo {
//    public final int running;
//    public final int offload_pending;
//    public final int offloaded;
//    public final int deoffloaded;
//    public final int totalStateless;
//    public final List<TaskExecutor> runningTasks;
//
//    public StatelessTaskStatInfo(
//      final int running, final int offload_pending, final int offloaded, final int deoffloaded,
//      final int totalStateless,
//      final List<TaskExecutor> runningTasks) {
//      this.running = running;
//      this.offload_pending = offload_pending;
//      this.offloaded = offloaded;
//      this.deoffloaded = deoffloaded;
//      this.totalStateless = totalStateless;
//      this.runningTasks = runningTasks;
//    }
//
//    public List<TaskExecutor> getRunningStatelessTasks() {
//      return runningTasks;
//    }
//  }
//
//  private StatelessTaskStatInfo measureTaskStatInfo() {
//    int running = 0;
//    int offpending = 0;
//    int offloaded = 0;
//    int deoffpending = 0;
//    int stateless = 0;
//    final List<TaskExecutor> runningTasks = new ArrayList<>(taskExecutorMap.size());
//     for (final TaskExecutor taskExecutor : taskExecutorMap.keySet()) {
//       if (taskExecutor.isStateless()) {
//         stateless += 1;
//         if (taskExecutor.isRunning()) {
//           runningTasks.add(taskExecutor);
//           running += 1;
//         } else if (taskExecutor.isOffloadPending()) {
//           offpending += 1;
//         } else if (taskExecutor.isOffloadedTask()) {
//           offloaded += 1;
//         } else if (taskExecutor.isDeoffloadPending()) {
//           deoffpending += 1;
//         }
//       }
//    }
//
//    LOG.info("Stateless Task running {}, offload_pending: {}, offloaded: {}, deoffload_pending: {}, total: {}",
//      running, offpending, offloaded, deoffpending, stateless);
//
//     return new StatelessTaskStatInfo(running, offpending, offloaded, deoffpending, stateless, runningTasks);
//  }
//
//  public void startDebugging() {
//    // For offloading debugging
//    se.scheduleAtFixedRate(() -> {
//      LOG.info("Start offloading kafka (only first stage)");
//      int cnt = 0;
//
//      final int offloadCnt = taskExecutorMap.keySet().stream()
//        .filter(taskExecutor -> taskExecutor.getId().startsWith("Stage0")).toArray().length - evalConf.minVmTask;
//
//      for (final TaskExecutor taskExecutor : taskExecutorMap.keySet()) {
//        if (taskExecutor.getId().startsWith("Stage0") && cnt < offloadCnt) {
//          LOG.info("Offload task {}, cnt: {}, offloadCnt: {}", taskExecutor.getId(), cnt, offloadCnt);
//          offloadedExecutors.add(Pair.of(taskExecutor, System.currentTimeMillis()));
//          taskExecutor.startOffloading(System.currentTimeMillis(), null, (m) -> {});
//          cnt += 1;
//        }
//      }
//    }, 25, 50, TimeUnit.SECONDS);
//
//    se.scheduleAtFixedRate(() -> {
//      LOG.info("End offloading kafka");
//      while (!offloadedExecutors.isEmpty()) {
//        final TaskExecutor endTask = offloadedExecutors.remove(0).left();
//        LOG.info("End task {}", endTask);
//        endTask.endOffloading((m) -> {
//          // do sth
//        }, false);
//      }
//    }, 40, 50, TimeUnit.SECONDS);
//  }
//
//
//  private Map<TaskExecutor, Long> calculateCpuTimeDelta(
//    final Map<TaskExecutor, Long> prevMap,
//    final Map<TaskExecutor, Long> currMap) {
//    final Map<TaskExecutor, Long> deltaMap = new HashMap<>(currMap);
//    for (final TaskExecutor key : prevMap.keySet()) {
//      final Long prevTaskTime = prevMap.get(key);
//      final Long currTaskTime = currMap.get(key) == null ? 0L : currMap.get(key);
//      deltaMap.put(key, currTaskTime - prevTaskTime);
//    }
//    return deltaMap;
//  }
//
//  private List<TaskExecutor> runningTasksInDeoffloadTimeOrder(final List<TaskExecutor> runningTasks) {
//    final List<TaskExecutor> tasks = runningTasks
//      .stream().filter(runningTask -> {
//        return !offloadedExecutors.stream().map(Pair::left).collect(Collectors.toSet()).contains(runningTask);
//      }).collect(Collectors.toList());
//
//    tasks.sort(new Comparator<TaskExecutor>() {
//      @Override
//      public int compare(TaskExecutor o1, TaskExecutor o2) {
//        return (int) (o1.getPrevOffloadEndTime().get() - o2.getPrevOffloadEndTime().get());
//      }
//    });
//
//    return tasks;
//  }
//
//  public void start() {
//    this.monitorThread.scheduleAtFixedRate(() -> {
//
//      try {
//        final double cpuLoad = profiler.getCpuLoad();
//
//        final Map<TaskExecutor, Long> currTaskCpuTimeMap = profiler.getTaskExecutorCpuTimeMap();
//        final Map<TaskExecutor, Long> deltaMap = calculateCpuTimeDelta(prevTaskCpuTimeMap, currTaskCpuTimeMap);
//        prevTaskCpuTimeMap = currTaskCpuTimeMap;
//
//        final Long elapsedCpuTimeSum = deltaMap.values().stream().reduce(0L, (x, y) -> x + y);
//
//        // calculate stable cpu time
//        if (cpuLoad >= 0.15 && cpuLoad <= 0.8) {
//          cpuLoadStable += 1;
//          if (cpuLoadStable >= 2) {
//            observedCnt += 1;
//            cpuTimeModel.add(cpuLoad, elapsedCpuTimeSum);
//          }
//        } else {
//          cpuLoadStable = 0;
//        }
//
//        cpuHighAverage.addValue(cpuLoad);
//        cpuLowAverage.addValue(cpuLoad);
//
//        final double cpuHighMean = cpuHighAverage.getMean();
//        final double cpuLowMean = cpuLowAverage.getMean();
//
//        final long currTime = System.currentTimeMillis();
//
//        final StatelessTaskStatInfo taskStatInfo = measureTaskStatInfo();
//        LOG.info("CpuHighMean: {}, CpuLowMean: {}, runningTask {}, threshold: {}, observed: {}",
//          cpuHighMean, cpuLowMean, taskStatInfo.running, threshold, observedCnt);
//
//        if (cpuHighMean < threshold) {
//          consecutive.set(0);
//        }
//
//        if (cpuHighMean > threshold) {
//          consecutive.getAndIncrement();
//        }
//
//        if (cpuHighMean > threshold && observedCnt >= observeWindow && consecutive.get() > k) {
//
//          final long targetCpuTime = cpuTimeModel
//            .desirableMetricForLoad(threshold - 0.05);
//
//          // Adjust current cpu time
//          // Minus the pending tasks!
//          long currCpuTimeSum = elapsedCpuTimeSum -
//            deltaMap.entrySet().stream().filter(entry -> entry.getKey().isOffloadPending())
//              .map(entry -> entry.getValue()).reduce(0L, (x,y) -> x+y);
//
//          final long avgCpuTimePerTask = currCpuTimeSum / (taskStatInfo.running);
//
//          LOG.info("currCpuTimeSum: {}, runningTasks: {}", currCpuTimeSum, taskStatInfo.runningTasks.size());
//          final List<TaskExecutor> runningTasks = runningTasksInDeoffloadTimeOrder(taskStatInfo.runningTasks);
//          final long curr = System.currentTimeMillis();
//          int cnt = 0;
//          for (final TaskExecutor runningTask : runningTasks) {
//            if (cnt < runningTasks.size() - 1) {
//
//              if (curr - runningTask.getPrevOffloadEndTime().get() > slackTime) {
//                //final long cpuTimeOfThisTask = deltaMap.get(runningTask);
//
//                LOG.info("CurrCpuSum: {}, Task {} cpu sum: {}, targetSum: {}",
//                  currCpuTimeSum, runningTask.getId(), avgCpuTimePerTask, targetCpuTime);
//
//                if (currCpuTimeSum - avgCpuTimePerTask >= targetCpuTime) {
//                  // offload this task!
//                  LOG.info("Offloading task {}", runningTask.getId());
//                  runningTask.startOffloading(currTime, null, (m) -> {});
//                  offloadedExecutors.add(Pair.of(runningTask, currTime));
//                  currCpuTimeSum -= avgCpuTimePerTask;
//
//                  cnt += 1;
//                }
//              }
//            }
//          }
//        }
//      } catch (final Exception e) {
//        e.printStackTrace();
//        throw new RuntimeException(e);
//      }
//    }, r, r, TimeUnit.MILLISECONDS);
//  }
//
//  public void close() {
//    monitorThread.shutdown();
//  }
//
//
//  // 어느 시점 (baseTime) 을 기준으로 fluctuation 하였는가?
//  private boolean isBursty(final long baseTime,
//                           final List<Pair<Long, Double>> processedEvents) {
//    final List<Double> beforeBaseTime = new ArrayList<>();
//    final List<Double> afterBaseTime = new ArrayList<>();
//
//    for (final Pair<Long, Double> pair : processedEvents) {
//      if (pair.left() < baseTime) {
//        beforeBaseTime.add(pair.right());
//      } else {
//        afterBaseTime.add(pair.right());
//      }
//    }
//
//    final double avgEventBeforeBaseTime = beforeBaseTime.stream()
//      .reduce(0.0, (x, y) -> x + y) / (Math.max(1, beforeBaseTime.size()));
//
//    final double avgEventAfterBaseTime = afterBaseTime.stream()
//      .reduce(0.0, (x, y) -> x + y) / (Math.max(1, afterBaseTime.size()));
//
//    LOG.info("avgEventBeforeBaseTime: {} (size: {}), avgEventAfterBaseTime: {} (size: {}), baseTime: {}",
//      avgEventBeforeBaseTime, beforeBaseTime.size(), avgEventAfterBaseTime, afterBaseTime.size(), baseTime);
//
//    processedEvents.clear();
//
//    if (avgEventBeforeBaseTime * 2 < avgEventAfterBaseTime) {
//      return true;
//    } else {
//      return false;
//    }
//  }
}
