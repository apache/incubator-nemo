package org.apache.nemo.runtime.executor.burstypolicy;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.nemo.common.exception.IllegalMessageException;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.offloading.common.OffloadingSerializer;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.TaskLoc;
import org.apache.nemo.runtime.common.TaskLocationMap;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageContext;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.MessageListener;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.runtime.executor.*;
import org.apache.nemo.runtime.executor.common.TaskExecutor;
import org.apache.nemo.runtime.lambdaexecutor.general.OffloadingExecutorSerializer;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.nemo.runtime.common.message.MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID;

public final class JobScalingHandlerWorker implements TaskOffloadingPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(JobScalingHandlerWorker.class.getName());

  // key: offloaded task executor, value: start time of offloading
  //private final List<Pair<TaskExecutor, Long>> offloadedExecutors;

  private final List<List<TaskExecutor>> offloadedTasksPerStage;

  private final ConcurrentMap<TaskExecutor, Boolean> taskExecutorMap;

  final PersistentConnectionToMasterMap toMaster;

  private int prevFileReadCnt = 0;

  private final StageOffloadingWorkerManager stageOffloadingWorkerManager;

  private final Map<TaskExecutor, DescriptiveStatistics> taskExecutionTimeMap = new HashMap<>();

  private final MessageEnvironment messageEnvironment;

  private final ExecutorService scalingService = Executors.newCachedThreadPool();

  private final String executorId;

  private TinyTaskOffloadingWorkerManager tinyWorkerManager;

  private final TaskLocationMap taskLocationMap;

  @Inject
  private JobScalingHandlerWorker(
    @Parameter(JobConf.ExecutorId.class) final String executorId,
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
    final MessageEnvironment messageEnvironment,
    final StageOffloadingWorkerManager stageOffloadingWorkerManager,
    final TaskLocationMap taskLocationMap) {
    this.taskLocationMap = taskLocationMap;
    this.executorId = executorId;
    this.stageOffloadingWorkerManager = stageOffloadingWorkerManager;
    this.taskExecutorMap = taskExecutorMapWrapper.getTaskExecutorMap();
    this.offloadedTasksPerStage = new ArrayList<>();
    this.messageEnvironment = messageEnvironment;

    this.toMaster = toMaster;
    LOG.info("Start JobScalingHandlerWorker");

    messageEnvironment.setupListener(SCALE_DECISION_MESSAGE_LISTENER_ID,
      new ScalingDecisionHandler());
  }

  public void setTinyWorkerManager(final TinyTaskOffloadingWorkerManager m) {
    tinyWorkerManager = m;
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

  private int largestLength(final List<List<TaskExecutor>> stageTasks) {
    int l = 0;
    for (final List<TaskExecutor> e : stageTasks) {
      if (e.size() > l) {
        l = e.size();
      }
    }

    return l;
  }

  private void scaleOut(final Map<String, List<String>> offloadTasks) {
    // scale out
    final StatelessTaskStatInfo taskStatInfo = PolicyUtils.measureTaskStatInfo(taskExecutorMap);

    final List<List<TaskExecutor>> stageTasks = stageTasks(taskStatInfo.runningTasks);

    for (final List<TaskExecutor> e : stageTasks) {
      final List<TaskExecutor> offloaded = new ArrayList<>();
      offloadedTasksPerStage.add(offloaded);
    }

    final RemainingOffloadTasks remainingOffloadTasks = RemainingOffloadTasks.getInstance();
    final int totalOffloadTasks = offloadTasks.values().stream()
      .map(x -> x.size()).reduce(0, (x,y) -> x+y);

    // 1. 여기서 먼저 worker 준비! 몇개 offloading할지 알고 있으니까 가능함.
    final OffloadingSerializer serializer = new OffloadingExecutorSerializer();

    remainingOffloadTasks.set(totalOffloadTasks);
    LOG.info("# of offloading tasks: {}", totalOffloadTasks);

    final int len = largestLength(stageTasks);

    final Map<String, Integer> offloadNumMap = new HashMap<>();

    for (int i = 0; i < len; i++) {
      for (int j = 0; j < stageTasks.size(); j++) {
        final List<TaskExecutor> te = stageTasks.get(j);

        if (te.size() > i) {
          final TaskExecutor task = te.get(i);
          final String stageId = RuntimeIdManager.getStageIdFromTaskId(task.getId());

          final List<String> stageOffloadTask = offloadTasks.get(stageId);

          if (stageOffloadTask.contains(task.getId())) {
            offloadedTasksPerStage.get(j).add(task);

            final int offloadNum = offloadNumMap.getOrDefault(stageId, 0);
            offloadNumMap.put(stageId, offloadNum + 1);

            final TinyTaskWorker worker = tinyWorkerManager.prepareSendTask(serializer);

            LOG.info("Offloading {}, cnt: {}, remainingOffloadTask: {}", task.getId(), offloadNum,
              remainingOffloadTasks.getRemainingCnt());

            task.startOffloading(System.currentTimeMillis(), worker, (m) -> {
              LOG.info("Offloading done for {}", task.getId());
              //stageOffloadingWorkerManager.endOffloading(stageId);
            });
          }
        }
      }
    }

    /*
    for (final List<TaskExecutor> tasks : stageTasks) {

      String stageId = "";

      if (tasks.size() > 0) {
        stageId = RuntimeIdManager.getStageIdFromTaskId(tasks.get(0).getId());
      }

      final int offloadCnt = scalingNum.getOrDefault(stageId, 0);
      LOG.info("Offloading {} tasks of {}", offloadCnt, stageId);

      int currentOffloadCnt = 0;

      for (final TaskExecutor runningTask : tasks) {
        if (currentOffloadCnt < offloadCnt) {

          offloaded.add(runningTask);

          currentOffloadCnt += 1;
          LOG.info("Offloading {}, cnt: {}, remainingOffloadTask: {}", runningTask.getId(), currentOffloadCnt,
            remainingOffloadTasks.getRemainingCnt());

          //offloadedExecutors.add(Pair.of(runningTask, System.currentTimeMillis()));

          runningTask.startOffloading(System.currentTimeMillis(), (m) -> {
            LOG.info("Offloading done for {}", runningTask.getId());
            //stageOffloadingWorkerManager.endOffloading(stageId);
          });
        }
      }
    }
    */

    LOG.info("Scale out method done {}", offloadedTasksPerStage);
  }

  private void scaleIn() {
    // scale in
    LOG.info("Offload tasks per stage: {}", offloadedTasksPerStage);

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

        offloadedTask.endOffloading((m) -> {
          // do sth
          LOG.info("Deoffloading done for {}", offloadedTask.getId());
          stageOffloadingWorkerManager.endOffloading(stageId);
        });
      }
    }

    offloadedTasksPerStage.clear();
  }

  @Override
  public void triggerPolicy() {

  }

  public void close() {
  }

  private final class ScalingDecisionHandler implements MessageListener<ControlMessage.Message> {

    @Override
    public void onMessage(final ControlMessage.Message message) {
      switch (message.getType()) {
        case RequestScaling:
          final ControlMessage.RequestScalingMessage scalingMsg = message.getRequestScalingMsg();

          if (scalingMsg.getIsScaleOut()) {

            final List<ControlMessage.TaskLocation> taskLocations = scalingMsg.getTaskLocationList();

            // update task location
            LOG.info("Updating task location...");
            taskLocationMap.locationMap.clear();
            for (final ControlMessage.TaskLocation taskLocation : taskLocations) {
              taskLocationMap.locationMap.put(taskLocation.getTaskId(),
                taskLocation.getIsVm() ? TaskLoc.VM : TaskLoc.SF);
            }

            final List<ControlMessage.RequestScalingEntryMessage> entries = scalingMsg.getEntryList();

            final Map<String, List<String>> scalingTaskMap = new HashMap<>();
            for (final ControlMessage.RequestScalingEntryMessage entry : entries) {
              scalingTaskMap.put(entry.getStageId(), entry.getOffloadTasksList());
            }

            LOG.info("Receive RequestScalingOut... {}", scalingTaskMap);

            if (!GlobalOffloadDone.getInstance().getBoolean().compareAndSet(true, false)) {
              throw new RuntimeException("GlobalOffloadDone should be true... but false TT");
            }

            scaleOut(scalingTaskMap);

            scalingService.execute(() -> {
              while (RemainingOffloadTasks.getInstance().getRemainingCnt() > 0) {
                // waiting...
                LOG.info("Waiting until finish input stop... cnt: {}",
                  RemainingOffloadTasks.getInstance().getRemainingCnt());

                try {
                  Thread.sleep(1000);
                } catch (InterruptedException e) {
                  e.printStackTrace();
                }
              }

              LOG.info("Send LocalScalingDone {}", executorId);

              // Send local offloading done
              toMaster.getMessageSender(SCALE_DECISION_MESSAGE_LISTENER_ID)
                .send(ControlMessage.Message.newBuilder()
                  .setId(RuntimeIdManager.generateMessageId())
                  .setListenerId(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID)
                  .setType(ControlMessage.MessageType.LocalScalingReadyDone)
                  .setLocalScalingDoneMsg(ControlMessage.LocalScalingDoneMessage.newBuilder()
                    .setExecutorId(executorId)
                    .build())
                  .build());

            });
          } else {
            // Scaling in
            LOG.info("Receive ScalingIn");
            scalingService.execute(JobScalingHandlerWorker.this::scaleIn);
          }
          break;
        case GlobalScalingReadyDone:
          LOG.info("Receive GlobalScalingReadyDone");
          if (!GlobalOffloadDone.getInstance().getBoolean().compareAndSet(false, true)) {
            throw new RuntimeException("Something wrong... offloadDone should be false,, but true");
          }

          break;
        default:
          throw new IllegalMessageException(
            new Exception("This message should not be received by an executor :" + message.getType()));
      }
    }

    @Override
    public void onMessageWithContext(final ControlMessage.Message message, final MessageContext messageContext) {
      switch (message.getType()) {
        default:
          throw new IllegalMessageException(
            new Exception("This message should not be requested to an executor :" + message.getType()));
      }
    }
  }
}
