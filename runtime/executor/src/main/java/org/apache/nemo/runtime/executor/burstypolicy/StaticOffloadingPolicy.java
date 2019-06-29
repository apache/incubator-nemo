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
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

public final class StaticOffloadingPolicy implements TaskOffloadingPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(StaticOffloadingPolicy.class.getName());

  // key: offloaded task executor, value: start time of offloading
  private final List<Pair<TaskExecutor, Long>> offloadedExecutors;

  private final ConcurrentMap<TaskExecutor, Boolean> taskExecutorMap;

  final PersistentConnectionToMasterMap toMaster;

  private int prevFileReadCnt = 0;

  private final StageOffloadingWorkerManager stageOffloadingWorkerManager;

  private final Map<TaskExecutor, DescriptiveStatistics> taskExecutionTimeMap = new HashMap<>();

  @Inject
  private StaticOffloadingPolicy(
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
    this.stageOffloadingWorkerManager = stageOffloadingWorkerManager;
    this.taskExecutorMap = taskExecutorMapWrapper.getTaskExecutorMap();
    this.offloadedExecutors = new ArrayList<>();

    this.toMaster = toMaster;
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
      final BufferedReader br =
        new BufferedReader(new FileReader("/home/ubuntu/incubator-nemo/scaling.txt"));

      String s;
      String lastLine = null;
      int cnt = 0;
      while ((s = br.readLine()) != null) {
        lastLine = s;
        cnt += 1;
      }

      if (cnt > prevFileReadCnt) {
        prevFileReadCnt = cnt;
        if (lastLine.equals("o")) {
          // scale out
          final StatelessTaskStatInfo taskStatInfo = PolicyUtils.measureTaskStatInfo(taskExecutorMap);
          int offloadCnt = 0;
          for (final TaskExecutor runningTask : taskStatInfo.runningTasks) {

            if (offloadCnt < taskStatInfo.runningTasks.size() / 2) {

              final String stageId = RuntimeIdManager.getStageIdFromTaskId(runningTask.getId());

              if (stageOffloadingWorkerManager.isStageOffloadable(stageId)) {
                //final long cpuTimeOfThisTask = deltaMap.get(runningTask);

                offloadCnt += 1;
                LOG.info("Offloading {}, cnt: {}", runningTask.getId(), offloadCnt);

                offloadedExecutors.add(Pair.of(runningTask, System.currentTimeMillis()));

                runningTask.startOffloading(System.currentTimeMillis(), (m) -> {
                  stageOffloadingWorkerManager.endOffloading(stageId);
                });
              }
            }
          }
        } else if (lastLine.equals("i")) {
          // scale in
          int offcnt = offloadedExecutors.size();
          for (final Pair<TaskExecutor, Long> pair : offloadedExecutors) {
            final TaskExecutor offloadedTask = pair.left();
            final String stageId = RuntimeIdManager.getStageIdFromTaskId(offloadedTask.getId());

            if (stageOffloadingWorkerManager.isStageOffloadable(stageId)) {

              offcnt -= 1;
              LOG.info("Deoffloading task {}, remaining offload: {}", offloadedTask.getId(), offcnt);

              offloadedTask.endOffloading((m) -> {
                // do sth
                stageOffloadingWorkerManager.endOffloading(stageId);
              });
            }
          }
          offloadedExecutors.clear();
        }
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public void close() {
  }
}
