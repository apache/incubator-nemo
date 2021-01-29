package org.apache.nemo.runtime.executor.burstypolicy;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.runtime.executor.*;
import org.apache.nemo.runtime.executor.common.TaskExecutor;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

public final class StaticSyncOffloadingPolicy implements TaskOffloadingPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(StaticSyncOffloadingPolicy.class.getName());

  // key: offloaded task executor, value: start time of offloading
  //private final List<Pair<TaskExecutor, Long>> offloadedExecutors;

  private final List<List<TaskExecutor>> offloadedTasksPerStage;

  private final ConcurrentMap<TaskExecutor, Boolean> taskExecutorMap;

  final PersistentConnectionToMasterMap toMaster;

  private int prevFileReadCnt = 0;

  private final StageOffloadingWorkerManager stageOffloadingWorkerManager;

  private final Map<TaskExecutor, DescriptiveStatistics> taskExecutionTimeMap = new HashMap<>();

  @Inject
  private StaticSyncOffloadingPolicy(
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
    this.offloadedTasksPerStage = new ArrayList<>();
    //this.offloadedExecutors = new ArrayList<>();

    this.toMaster = toMaster;
    LOG.info("Start StaticOffloadingPolicy");

    try {
      BufferedWriter writer = new BufferedWriter(new FileWriter("/home/ubuntu/incubator-nemo/scaling.txt"));
      writer.close();

      final BufferedReader br =
        new BufferedReader(new FileReader("/home/ubuntu/incubator-nemo/scaling.txt"));

      String s;
      String lastLine = null;
      int cnt = 0;
      while ((s = br.readLine()) != null) {
        lastLine = s;
        cnt += 1;
      }

      br.close();
      prevFileReadCnt = cnt;
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
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

  private List<List<TaskExecutor>> stageTasks(final List<TaskExecutor> runningTasks) {
    final Map<String, List<TaskExecutor>> map = new HashMap<>();
    final List<String> stages = new ArrayList<>();

    for (final TaskExecutor runningTask : runningTasks) {
      final String stageId = RuntimeIdManager.getStageIdFromTaskId(runningTask.getId());
      if (!map.containsKey(stageId)) {
        map.put(stageId, new ArrayList<>());
        stages.add(stageId);
      }

      map.get(stageId).add(runningTask);
    }

    stages.sort(String::compareTo);
    final List<List<TaskExecutor>> results = new ArrayList<>(stages.size());
    for (final String stageId : stages) {
      results.add(map.get(stageId));
    }

    return results;
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

      br.close();


      if (cnt > prevFileReadCnt) {
        prevFileReadCnt = cnt;

        String[] split = lastLine.split(" ");
        final String decision = split[0];

        if (decision.equals("o")) {
          final int offloadDivide = Integer.valueOf(split[1]);

          // scale out
          final StatelessTaskStatInfo taskStatInfo = PolicyUtils.measureTaskStatInfo(taskExecutorMap);

          final List<List<TaskExecutor>> stageTasks = stageTasks(taskStatInfo.runningTasks);
          final List<TaskExecutor> offloaded = new ArrayList<>();
          offloadedTasksPerStage.add(offloaded);

          final RemainingOffloadTasks remainingOffloadTasks = RemainingOffloadTasks.getInstance();

          // Set total offload task cnt
          int totalOffloadTasks = 0;
          for (final List<TaskExecutor> tasks : stageTasks) {
            final int remain = tasks.size() / offloadDivide;
            final int offcnt = tasks.size()  - remain;
            LOG.info("Tasks: {}, offcnt: {}, totalTAsks: {}", tasks.size(), offcnt, totalOffloadTasks);
            totalOffloadTasks += offcnt;
          }

          remainingOffloadTasks.set(totalOffloadTasks);

          LOG.info("# of offloading tasks: {}, offload divide: {}", totalOffloadTasks, offloadDivide);

          for (final List<TaskExecutor> tasks : stageTasks) {
            int offloadCnt = 0;

            final int remain = tasks.size() / offloadDivide;
            final int offcnt = tasks.size()  - remain;

            for (final TaskExecutor runningTask : tasks) {
              if (offloadCnt < offcnt) {

                offloaded.add(runningTask);
                final String stageId = RuntimeIdManager.getStageIdFromTaskId(runningTask.getId());

                offloadCnt += 1;
                LOG.info("Offloading {}, cnt: {}, remainingOffloadTask: {}", runningTask.getId(), offloadCnt,
                  remainingOffloadTasks.getRemainingCnt());

                //offloadedExecutors.add(Pair.of(runningTask, System.currentTimeMillis()));

                /*
                runningTask.startOffloading(System.currentTimeMillis(), null, (m) -> {
                  LOG.info("Offloading done for {}", runningTask.getId());
                  //stageOffloadingWorkerManager.endOffloading(stageId);
                });
                */
              }
            }
          }

        } else if (decision.equals("i")) {
          // scale in
          for (final List<TaskExecutor> offloadedTasks : offloadedTasksPerStage) {
            int offcnt = offloadedTasks.size();
            for (final TaskExecutor offloadedTask : offloadedTasks) {
              final String stageId = RuntimeIdManager.getStageIdFromTaskId(offloadedTask.getId());

              while (!stageOffloadingWorkerManager.isStageOffloadable(stageId)) {
                // waiting for stage offloading
                LOG.info("Waiting for stage deoffloading {}", stageId);
                try {
                  Thread.sleep(1000);
                } catch (InterruptedException e) {
                  e.printStackTrace();
                  throw new RuntimeException(e);
                }
              }

              offcnt -= 1;
              LOG.info("Deoffloading task {}, remaining offload: {}", offloadedTask.getId(), offcnt);

              /*
              offloadedTask.endOffloading((m) -> {
                // do sth
                LOG.info("Deoffloading done for {}", offloadedTask.getId());
                stageOffloadingWorkerManager.endOffloading(stageId);
              }, false);
              */
            }
          }

          offloadedTasksPerStage.clear();
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
