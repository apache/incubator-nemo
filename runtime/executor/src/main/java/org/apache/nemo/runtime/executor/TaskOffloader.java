package org.apache.nemo.runtime.executor;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.nemo.common.Pair;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.runtime.executor.burstypolicy.TaskOffloadingPolicy;
import org.apache.nemo.runtime.executor.common.TaskExecutor;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public final class TaskOffloader {
  private static final Logger LOG = LoggerFactory.getLogger(TaskOffloader.class.getName());

  private final ScheduledExecutorService monitorThread;
  private final SystemLoadProfiler profiler;

  private final long r;

  // key: offloaded task executor, value: start time of offloading
  private final List<Pair<TaskExecutor, Long>> offloadedExecutors;
  private final ConcurrentMap<TaskExecutor, Boolean> taskExecutorMap;

  private final DescriptiveStatistics cpuHighAverage;
  private final DescriptiveStatistics cpuLowAverage;
  private final DescriptiveStatistics eventAverage;

  // DEBUGGIGN
  final ScheduledExecutorService se = Executors.newSingleThreadScheduledExecutor();

  // TODO: high threshold
  // TODO: low threshold ==> threshold 2개 놓기

  final PersistentConnectionToMasterMap toMaster;
  private final StageOffloadingWorkerManager stageOffloadingWorkerManager;
  private final TaskOffloadingPolicy offloadingPolicy;

  @Inject
  private TaskOffloader(
    @Parameter(JobConf.ExecutorId.class) String executorId,
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
    final StageOffloadingWorkerManager stageOffloadingWorkerManager,
    final TaskOffloadingPolicy offloadingPolicy) {
    this.stageOffloadingWorkerManager = stageOffloadingWorkerManager;
    this.offloadingPolicy = offloadingPolicy;
    this.r = r;
    this.profiler = profiler;
    this.monitorThread = Executors.newSingleThreadScheduledExecutor();
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

  private void offloading(String stageId, int time, final int cnt) {
    se.schedule(() -> {
      LOG.info("Start offloading {}", stageId);

      //final int offloadCnt = taskExecutorMap.keySet().stream()
      //  .filter(taskExecutor -> taskExecutor.getId().startsWith("Stage0")).toArray().length - evalConf.minVmTask;
      int offloadCnt = 0;

      for (final TaskExecutor taskExecutor : taskExecutorMap.keySet()) {
        if (taskExecutor.getId().contains(stageId)
          &&  taskExecutor.isRunning()
          && offloadCnt < cnt
          && stageOffloadingWorkerManager.isStageOffloadable(stageId)) {
          //LOG.info("Offload task {}, cnt: {}, offloadCnt: {}", taskExecutor.getId(), cnt, offloadCnt);
          LOG.info("Offloading task {}", taskExecutor.getId());
          offloadedExecutors.add(Pair.of(taskExecutor, System.currentTimeMillis()));
          taskExecutor.startOffloading(System.currentTimeMillis(), (m) -> {
            stageOffloadingWorkerManager.endOffloading(stageId);
          });
          offloadCnt += 1;
        }
      }
    }, time, TimeUnit.SECONDS);
  }

  private void deoffloading(String stageId, int time, final int cnt) {
    se.schedule(() -> {
      LOG.info("Start deoffloading {}", stageId);
      int deoffloadCnt = 0;

      //final int offloadCnt = taskExecutorMap.keySet().stream()
      //  .filter(taskExecutor -> taskExecutor.getId().startsWith("Stage0")).toArray().length - evalConf.minVmTask;

      final Iterator<Pair<TaskExecutor, Long>> iterator = offloadedExecutors.iterator();
      while (iterator.hasNext()) {
        final Pair<TaskExecutor, Long> pair = iterator.next();
        if (pair.left().getId().contains(stageId)) {
          if (deoffloadCnt < cnt && stageOffloadingWorkerManager.isStageOffloadable(stageId)) {
            LOG.info("Deoffloading {}", pair.left().getId());
            pair.left().endOffloading((m) -> {
              LOG.info("Receive end offloading of {} ... send offloding done event", pair.left().getId());
              stageOffloadingWorkerManager.endOffloading(stageId);
              //sendOffloadingDoneEvent(pair.left().getId());
            });
            iterator.remove();
            deoffloadCnt += 1;
          }
        }
      }
    }, time, TimeUnit.SECONDS);
  }

  public void startDownstreamDebugging() {
    // For offloading debugging
    offloading("Stage0", 30, 8);
    offloading("Stage1", 40, 8);
    offloading("Stage2", 50, 8);


    deoffloading("Stage0", 80, 8);
    deoffloading("Stage1", 90, 8);
    deoffloading("Stage2", 100, 8);

        // For offloading debugging
    offloading("Stage0", 130, 8);
    offloading("Stage1", 140, 8);
    offloading("Stage2", 150, 8);


    deoffloading("Stage0", 180, 8);
    deoffloading("Stage1", 190, 8);
    deoffloading("Stage2", 200, 8);

        // For offloading debugging
    offloading("Stage0", 230, 8);
    offloading("Stage1", 240, 8);
    offloading("Stage2", 250, 8);


    deoffloading("Stage0", 280, 8);
    deoffloading("Stage1", 290, 8);
    deoffloading("Stage2", 300, 8);


    //offloading("Stage1", 80, 5);
    //deoffloading("Stage1", 85, 5);

    /*
    offloading("Stage0", 85, 3);

    deoffloading("Stage0", 110, 5);
    */
    //deoffloading("Stage1", 140);


    /*
    offloading("Stage0", 115);

    deoffloading("Stage2", 135);
    deoffloading("Stage0", 160);
    */
  }

  private Map<TaskExecutor, Long> calculateCpuTimeDelta(
    final Map<TaskExecutor, Long> prevMap,
    final Map<TaskExecutor, Long> currMap) {
    final Map<TaskExecutor, Long> deltaMap = new HashMap<>(currMap);
    for (final TaskExecutor key : prevMap.keySet()) {
      final Long prevTaskTime = prevMap.get(key);
      final Long currTaskTime = currMap.get(key) == null ? 0L : currMap.get(key);
      deltaMap.put(key, currTaskTime - prevTaskTime);
    }
    return deltaMap;
  }

  public void start() {
    this.monitorThread.scheduleAtFixedRate(() -> {
      offloadingPolicy.triggerPolicy();
    }, r, r, TimeUnit.MILLISECONDS);
  }

  public void close() {
    monitorThread.shutdown();
    offloadingPolicy.close();
  }
}
