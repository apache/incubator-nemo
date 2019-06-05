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

public final class StepwiseOffloadingPolicy implements TaskOffloadingPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(StepwiseOffloadingPolicy.class.getName());

  private final SystemLoadProfiler profiler;

  private final long r;
  private final int k;
  private final double threshold;

  // key: offloaded task executor, value: start time of offloading
  private final List<Pair<TaskExecutor, Long>> offloadedExecutors;
  private final ConcurrentMap<TaskExecutor, Boolean> taskExecutorMap;
  private long slackTime = 10000;
  private long deoffloadSlackTime = 15000;


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
  private final int observeWindow = 20;


  private long prevDeOffloadingTime = System.currentTimeMillis();
  final PersistentConnectionToMasterMap toMaster;

  private final StageOffloadingWorkerManager stageOffloadingWorkerManager;

  private int multiplicativeOffloading = 0;

  private final int baseOffloadingTaskNum = 10;

  private final AtomicInteger offloadingPendingCnt = new AtomicInteger(0);
  private final AtomicInteger deoffloadingPendingCnt = new AtomicInteger(0);

  @Inject
  private StepwiseOffloadingPolicy(
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
    this.offloadedExecutors = new LinkedList<>();

    this.toMaster = toMaster;
  }

  private List<TaskExecutor> runningTasksInCpuTimeOrder(
    final List<TaskExecutor> runningTasks,
    final Map<TaskExecutor, Long> deltaMap) {

    final List<TaskExecutor> tasks = runningTasks
      .stream().filter(runningTask -> {
        return !offloadedExecutors.stream().map(Pair::left).collect(Collectors.toSet()).contains(runningTask);
      }).collect(Collectors.toList());

    tasks.sort(new Comparator<TaskExecutor>() {
      @Override
      public int compare(TaskExecutor o1, TaskExecutor o2) {
        return (int) (deltaMap.get(o2) - deltaMap.get(o1));
      }
    });

    return tasks;
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

      long elapsedCpuTimeSum = 0L;
      for (final Long val : deltaMap.values()) {
        elapsedCpuTimeSum += (val / 1000);
      }

      // calculate stable cpu time
      if (cpuLoad >= 0.28 && elapsedCpuTimeSum > 100000) {
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

      final StatelessTaskStatInfo taskStatInfo = PolicyUtils.measureTaskStatInfo(taskExecutorMap);


      LOG.info("CPU Load: {}, Elapsed Time: {}, Off pending: {}, Deoff pending: {}, runningTasks: {}, offloaded: {}, " +
          "off_pending: {}. deoff_pending: {}"
        , cpuLoad, elapsedCpuTimeSum, offloadingPendingCnt, deoffloadingPendingCnt, taskStatInfo.running,
        taskStatInfo.offloaded, taskStatInfo.offload_pending, taskStatInfo.deoffloaded);

      //LOG.info("CpuHighMean: {}, CpuLowMean: {}, runningTask {}, threshold: {}, observed: {}, offloaded: {}",
      //  cpuHighMean, cpuLowMean, taskStatInfo.running, threshold, observedCnt);

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

      if (cpuHighMean > threshold && observedCnt >= observeWindow
        && deoffloadingPendingCnt.get() == 0
        && System.currentTimeMillis() - prevDeOffloadingTime >= deoffloadSlackTime) {


        //cpuTimeModel
        //  .desirableMetricForLoad((threshold + evalConf.deoffloadingThreshold) / 2.0);

        // Adjust current cpu time
        // Minus the pending tasks!
        long currCpuTimeSum = 0;
        // correct
        // jst running worker
        for (final Map.Entry<TaskExecutor, Long> entry : deltaMap.entrySet()) {
          if (entry.getKey().isRunning()) {
            currCpuTimeSum  += entry.getValue() / 1000;
          }
        }

        //final long avgCpuTimePerTask = currCpuTimeSum / (taskStatInfo.running);

        //LOG.info("currCpuTimeSum: {}, runningTasks: {}", currCpuTimeSum, taskStatInfo.runningTasks.size());
        //final List<TaskExecutor> runningTasks = runningTasksInDeoffloadTimeOrder(taskStatInfo.runningTasks);
        final List<TaskExecutor> runningTasks = runningTasksInCpuTimeOrder(taskStatInfo.statelessRunningTasks, deltaMap);
        final long curr = System.currentTimeMillis();
        int cnt = 0;
        //final int multiple = baseOffloadingTaskNum * (int) Math.pow(2, multiplicativeOffloading);
        final double percentage = Math.min(0.9, 0.1 * (multiplicativeOffloading + 1));
        final int multiple = (int) (runningTasks.size() * percentage);

        LOG.info("Running tasks: {}, percentage:{}, multiple: {}", runningTasks.size(), percentage, multiple);

        for (final TaskExecutor runningTask : runningTasks) {
          final long currTaskCpuTime = deltaMap.get(runningTask) / 1000;
          //if (cnt < runningTasks.size() - 1) {

          if (cnt < multiple) {

            final String stageId = RuntimeIdManager.getStageIdFromTaskId(runningTask.getId());

            if (stageOffloadingWorkerManager.isStageOffloadable(stageId)) {
              //final long cpuTimeOfThisTask = deltaMap.get(runningTask);

              // offload this task!
              LOG.info("Offloading task {}, cnt: {}, multiple: {}, percentage: {}",
                runningTask.getId(), cnt, multiple, multiplicativeOffloading);

              offloadingPendingCnt.getAndIncrement();

              runningTask.startOffloading(System.currentTimeMillis(), (m) -> {
                stageOffloadingWorkerManager.endOffloading(stageId);
                offloadingPendingCnt.decrementAndGet();
              });

              offloadedExecutors.add(Pair.of(runningTask, currTime));
              currCpuTimeSum -= currTaskCpuTime;

              cnt += 1;

            }
          }
        }

        multiplicativeOffloading += 1;

      } else if (cpuLowMean < evalConf.deoffloadingThreshold  &&  observedCnt >= observeWindow
        && deoffloadingPendingCnt.get() == 0) {
        multiplicativeOffloading = 0;

        if (!offloadedExecutors.isEmpty()) {
          //final long targetCpuTime = cpuTimeModel.desirableMetricForLoad((threshold + evalConf.deoffloadingThreshold) / 2.0);
          final long targetCpuTime = 3000000;

          long currCpuTimeSum = 0;
          // correct
          // jst running worker
          for (final Map.Entry<TaskExecutor, Long> entry : deltaMap.entrySet()) {
            if (entry.getKey().isRunning()) {
              currCpuTimeSum  += entry.getValue() / 1000;
            } else if (entry.getKey().isDeoffloadPending()) {
              currCpuTimeSum  += (entry.getKey().calculateOffloadedTaskTime() / 1000);
            }
          }

          // add deoffload pending
          final Map<String, Boolean> stageOffloadableMap = new HashMap<>();
          final Map<String, AtomicInteger> stageOffloadingCntMap = new HashMap<>();

          LOG.info("Try to deoffload... currCpuTimeSum: {}, targetCpuTime: {}", currCpuTimeSum, targetCpuTime);
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

                if (stageOffloadingWorkerManager.isStageOffloadable(stageId)) {

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
                    LOG.info("Deoffloading done for task {}", taskExecutor.getId());

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
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public void close() {
  }
}