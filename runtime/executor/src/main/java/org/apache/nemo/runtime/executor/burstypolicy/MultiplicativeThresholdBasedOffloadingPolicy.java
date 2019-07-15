package org.apache.nemo.runtime.executor.burstypolicy;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.nemo.common.Pair;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.runtime.executor.*;
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

public final class MultiplicativeThresholdBasedOffloadingPolicy implements TaskOffloadingPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(MultiplicativeThresholdBasedOffloadingPolicy.class.getName());

  private final SystemLoadProfiler profiler;

  private final long r;
  private final int k;
  private final double threshold;

  // key: offloaded task executor, value: start time of offloading
  private final List<Pair<TaskExecutor, Long>> offloadedExecutors;
  private final List<TaskExecutor> offloadPendingExecutors;

  private final ConcurrentMap<TaskExecutor, Boolean> taskExecutorMap;
  private long slackTime = 10000;
  private long deoffloadSlackTime = 20000;


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
  private final int observeWindow = 10;

  private long prevDeOffloadingTime = System.currentTimeMillis();
  final PersistentConnectionToMasterMap toMaster;

  private final StageOffloadingWorkerManager stageOffloadingWorkerManager;
  private final AtomicInteger offloadingPendingCnt = new AtomicInteger(0);
  private final AtomicInteger deoffloadingPendingCnt = new AtomicInteger(0);

  private final Map<TaskExecutor, DescriptiveStatistics> taskExecutionTimeMap = new HashMap<>();

  @Inject
  private MultiplicativeThresholdBasedOffloadingPolicy(
    final SystemLoadProfiler profiler,
    @Parameter(EvalConf.BottleneckDetectionPeriod.class) final long r,
    @Parameter(EvalConf.BottleneckDetectionConsecutive.class) final int k,
    @Parameter(EvalConf.BottleneckDetectionCpuThreshold.class) final double threshold,
    final TaskEventRateCalculator taskEventRateCalculator,
    final TaskExecutorMapWrapper taskExecutorMapWrapper,
    final CpuEventModel cpuEventModel,
    final PolynomialCpuTimeModel cpuTimeModel,
    final EvalConf evalConf,
    final PersistentConnectionToMasterMap toMaster,
    final StageOffloadingWorkerManager stageOffloadingWorkerManager) {
    this.evalConf = evalConf;
    this.stageOffloadingWorkerManager = stageOffloadingWorkerManager;
    this.r = r;
    this.k = k;
    this.threshold = threshold;
    this.profiler = profiler;
    this.cpuTimeModel = cpuTimeModel;
    this.cpuHighAverage = new DescriptiveStatistics();
    cpuHighAverage.setWindowSize(2);
    this.cpuLowAverage = new DescriptiveStatistics();
    cpuLowAverage.setWindowSize(2);

    this.eventAverage = new DescriptiveStatistics();
    eventAverage.setWindowSize(2);

    this.taskExecutorMap = taskExecutorMapWrapper.getTaskExecutorMap();
    this.offloadedExecutors = new ArrayList<>();
    this.offloadPendingExecutors = new ArrayList<>();

    this.toMaster = toMaster;
  }

  private List<TaskExecutor> runningTasksInCpuTimeOrder(
    final List<TaskExecutor> runningTasks) {

    final List<TaskExecutor> tasks = runningTasks
      .stream().filter(runningTask -> {
        return !offloadedExecutors.stream().map(Pair::left).collect(Collectors.toSet()).contains(runningTask) &&
          !offloadPendingExecutors.stream().collect(Collectors.toSet()).contains(runningTask);
      }).collect(Collectors.toList());

    tasks.sort(new Comparator<TaskExecutor>() {
      @Override
      public int compare(TaskExecutor o1, TaskExecutor o2) {
        return (int) (taskExecutionTimeMap.get(o2).getMean() - taskExecutionTimeMap.get(o1).getMean());
      }
    });

    return tasks;
  }

  private boolean checkAllInputStop() {
    synchronized (offloadPendingExecutors) {
      for (final TaskExecutor executor : offloadPendingExecutors) {
        if (!executor.getPendingStatus().equals(TaskExecutor.PendingState.OUTPUT_PENDING)) {
          return false;
        }
      }
    }
    return true;
  }

  private void updateTaskExecutionTime(final Map<TaskExecutor, Long> deltaMap) {
    for (final Map.Entry<TaskExecutor, Long> entry : deltaMap.entrySet()) {
      final TaskExecutor executor = entry.getKey();
      final Long time = entry.getValue();

      if (!taskExecutionTimeMap.containsKey(executor)) {
        final DescriptiveStatistics s = new DescriptiveStatistics();
        s.setWindowSize(3);
        taskExecutionTimeMap.put(executor, s);
      }

      final DescriptiveStatistics stat = taskExecutionTimeMap.get(executor);
      stat.addValue(time / 1000);
    }
  }

  @Override
  public void triggerPolicy() {
    try {
      final double cpuLoad = profiler.getCpuLoad();

      final Map<TaskExecutor, Long> deltaMap =
        taskExecutorMap.keySet().stream()
          .map(taskExecutor -> {
            final long executionTime = taskExecutor.getTaskExecutionTime().get();
            taskExecutor.getTaskExecutionTime().getAndAdd(-executionTime);
            return Pair.of(taskExecutor, executionTime);
          }).collect(Collectors.toMap(Pair::left, Pair::right));

      // update time
      updateTaskExecutionTime(deltaMap);

      long elapsedCpuTimeSum = 0L;
      for (final DescriptiveStatistics val : taskExecutionTimeMap.values()) {
        elapsedCpuTimeSum += val.getMean();
      }

      // calculate stable cpu time
      if (cpuLoad >= 0.28) {
        cpuLoadStable += 1;
        if (cpuLoadStable >= 2) {
          observedCnt += 1;
          cpuTimeModel.add(cpuLoad, elapsedCpuTimeSum);
        }
      } else {
        cpuLoadStable = 0;
      }

      cpuHighAverage.addValue(cpuLoad);
      cpuLowAverage.addValue(cpuLoad);

      final double cpuHighMean = cpuHighAverage.getMean();
      final double cpuLowMean = cpuLowAverage.getMean();

      final long currTime = System.currentTimeMillis();

      LOG.info("CPU Load: {}, Elapsed Time: {}", cpuLoad, elapsedCpuTimeSum);

      final StatelessTaskStatInfo taskStatInfo = PolicyUtils.measureTaskStatInfo(taskExecutorMap);
      LOG.info("CpuHighMean: {}, CpuLowMean: {}, runningTask {}, threshold: {}, observed: {}, offloaded: {}",
        cpuHighMean, cpuLowMean, taskStatInfo.running, threshold, observedCnt);

      if (!offloadedExecutors.isEmpty()) {
        final long cur = System.currentTimeMillis();
        final Iterator<Pair<TaskExecutor, Long>> it = offloadedExecutors.iterator();
        while (it.hasNext()) {
          final Pair<TaskExecutor, Long> elem = it.next();
          if (cur - elem.right() >= TimeUnit.SECONDS.toMillis(1000)) {
            // force close!
            LOG.info("Force close workers !! {}, {}", elem.left(), elem.right());
            elem.left().endOffloading((m) -> {
              // do sth
            });
            it.remove();
            prevDeOffloadingTime = System.currentTimeMillis();
          }
        }
      }

      if (cpuHighMean > threshold && observedCnt >= observeWindow &&
        System.currentTimeMillis() - prevDeOffloadingTime >= slackTime) {

        final double targetCPuLoad = ((threshold + evalConf.deoffloadingThreshold) / 2.0) - 0.05;
        final long targetCpuTime = cpuTimeModel
          .desirableMetricForLoad(targetCPuLoad);

        // Adjust current cpu time
        // Minus the pending tasks!
        long currCpuTimeSum = 0;
        // correct
        // jst running worker
        for (final Map.Entry<TaskExecutor, DescriptiveStatistics> entry : taskExecutionTimeMap.entrySet()) {
          if (entry.getKey().isRunning()) {
            currCpuTimeSum  += entry.getValue().getMean();
          }
        }

        //final long avgCpuTimePerTask = currCpuTimeSum / (taskStatInfo.running);

        LOG.info("currCpuTimeSum: {}, runningTasks: {}", currCpuTimeSum, taskStatInfo.runningTasks.size());
        //final List<TaskExecutor> runningTasks = runningTasksInDeoffloadTimeOrder(taskStatInfo.runningTasks);
        final List<TaskExecutor> runningTasks = runningTasksInCpuTimeOrder(taskStatInfo.runningTasks);
        final long curr = System.currentTimeMillis();
        int cnt = 0;

        for (final TaskExecutor runningTask : runningTasks) {
          final long currTaskCpuTime = (long) taskExecutionTimeMap.get(runningTask).getMean();
          //if (cnt < runningTasks.size() - 1) {

          if (curr - runningTask.getPrevOffloadEndTime().get() > slackTime &&
            currCpuTimeSum > targetCpuTime) {

            final String stageId = RuntimeIdManager.getStageIdFromTaskId(runningTask.getId());

            if (stageOffloadingWorkerManager.isStageOffloadable(stageId)) {
              //final long cpuTimeOfThisTask = deltaMap.get(runningTask);

              LOG.info("CurrCpuSum: {}, Task {} cpu sum: {}, targetSum: {}",
                currCpuTimeSum, runningTask.getId(), currTaskCpuTime, targetCpuTime);

              synchronized (offloadPendingExecutors) {
                offloadPendingExecutors.add(runningTask);
              }

              // offload this task!
              LOG.info("Offloading task {}", runningTask.getId());
              offloadingPendingCnt.getAndIncrement();

              runningTask.startOffloading(System.currentTimeMillis(), null, (m) -> {
                stageOffloadingWorkerManager.endOffloading(stageId);
                offloadingPendingCnt.decrementAndGet();

                runningTask.getPrevOffloadStartTime().set(System.currentTimeMillis());

                synchronized (offloadPendingExecutors) {
                  offloadPendingExecutors.remove(runningTask);
                }

                synchronized (offloadedExecutors) {
                  offloadedExecutors.add(Pair.of(runningTask, currTime));
                }
              });

              currCpuTimeSum -= currTaskCpuTime;

              cnt += 1;

            }
          }
        }

      } else if (cpuLowMean < evalConf.deoffloadingThreshold  &&  observedCnt >= observeWindow
        && deoffloadingPendingCnt.get() == 0) {
        if (!offloadedExecutors.isEmpty()) {
          final long targetCpuTime = cpuTimeModel.desirableMetricForLoad((threshold + evalConf.deoffloadingThreshold) / 2.0);
          //final long targetCpuTime = 5000000;

          long currCpuTimeSum = 0;
          // correct
          // jst running worker
          for (final Map.Entry<TaskExecutor, DescriptiveStatistics> entry : taskExecutionTimeMap.entrySet()) {
            if (entry.getKey().isRunning()) {
              currCpuTimeSum += entry.getValue().getMean();
            } else if (entry.getKey().isDeoffloadPending()) {
              currCpuTimeSum += (entry.getKey().calculateOffloadedTaskTime() / 1000);
            }
          }

          // add deoffload pending
          final Map<String, AtomicInteger> stageOffloadingCntMap = new HashMap<>();

          LOG.info("Try to deoffload... currCpuTimeSum: {}, targetCpuTime: {}", currCpuTimeSum, targetCpuTime);
          synchronized (offloadedExecutors) {

            final Iterator<Pair<TaskExecutor, Long>> iterator = offloadedExecutors.iterator();

            while (iterator.hasNext() && currCpuTimeSum < targetCpuTime) {

              final Pair<TaskExecutor, Long> pair = iterator.next();
              final TaskExecutor taskExecutor = pair.left();
              if (taskExecutor.isOffloaded()) {
                final Long offloadingTime = taskExecutor.getPrevOffloadStartTime().get();
                final long avgCpuTimeSum = taskExecutor.calculateOffloadedTaskTime() / 1000;

                final String stageId = RuntimeIdManager.getStageIdFromTaskId(taskExecutor.getId());

                if (avgCpuTimeSum > 0) {
                  LOG.info("Deoff] CurrCpuSum: {}, Task {} avg cpu sum: {}, targetSum: {}",
                    currCpuTimeSum, taskExecutor.getId(), avgCpuTimeSum, targetCpuTime);

                  if (currTime - offloadingTime >= deoffloadSlackTime
                    && stageOffloadingWorkerManager.isStageOffloadable(stageId)) {

                    final AtomicInteger cnt =
                      stageOffloadingCntMap.getOrDefault(stageId, new AtomicInteger(0));

                    cnt.getAndIncrement();
                    stageOffloadingCntMap.putIfAbsent(stageId, cnt);

                    LOG.info("Deoffloading task {}, currCpuTime: {}, avgCpuSUm: {}",
                      taskExecutor.getId(), currCpuTimeSum, avgCpuTimeSum);
                    iterator.remove();

                    deoffloadingPendingCnt.getAndIncrement();

                    taskExecutor.endOffloading((m) -> {
                      // do sth
                      stageOffloadingWorkerManager.endOffloading(stageId);
                      deoffloadingPendingCnt.decrementAndGet();
                    });
                    currCpuTimeSum += avgCpuTimeSum;
                    prevDeOffloadingTime = System.currentTimeMillis();
                  }
                }
              } else if (taskExecutor.isOffloadPending()) {
                LOG.info("Tas {} is offload pending... ", taskExecutor.getId());
                /*
                // pending means that it is not offloaded yet.
                // close immediately!
                LOG.info("Immediately deoffloading!");
                taskExecutor.endOffloading((m) -> {
                  // do sth
                });
                iterator.remove();
                */
              }
            }
          }
        }
      }
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public void close() {
  }
}
