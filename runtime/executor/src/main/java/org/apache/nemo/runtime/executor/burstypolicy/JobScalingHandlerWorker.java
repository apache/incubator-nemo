package org.apache.nemo.runtime.executor.burstypolicy;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.nemo.common.exception.IllegalMessageException;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.TaskLoc;
import org.apache.nemo.common.TaskLocationMap;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageContext;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.MessageListener;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.runtime.executor.*;
import org.apache.nemo.runtime.executor.common.TaskExecutor;
import org.apache.nemo.runtime.executor.data.PipeManagerWorker;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerAddr;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerPort;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.nemo.common.TaskLoc.SF;
import static org.apache.nemo.common.TaskLoc.VM;
import static org.apache.nemo.common.TaskLoc.VM_SCALING;
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

  private final StageExecutorThreadMap stageExecutorThreadMap;

  private final ExecutorThreads executorThreads;

  private final EvalConf evalConf;

  private final ScalingOutCounter scalingOutCounter;

  private final SFTaskMetrics sfTaskMetrics;

  private final PipeManagerWorker pipeManagerWorker;

  private final String nameServerAddr;
  private final int nameServerPort;

  @Inject
  private JobScalingHandlerWorker(
    @Parameter(NameResolverNameServerAddr.class) final String serverAddr,
    @Parameter(NameResolverNameServerPort.class) final int serverPort,
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
    final TaskLocationMap taskLocationMap,
    final StageExecutorThreadMap stageExecutorThreadMap,
    final ExecutorThreads executorThreads,
    final ScalingOutCounter scalingOutCounter,
    final SFTaskMetrics sfTaskMetrics,
    final PipeManagerWorker pipeManagerWorker) {
    this.nameServerAddr = serverAddr;
    this.nameServerPort = serverPort;
    this.taskLocationMap = taskLocationMap;
    this.executorId = executorId;
    this.stageOffloadingWorkerManager = stageOffloadingWorkerManager;
    this.taskExecutorMap = taskExecutorMapWrapper.getTaskExecutorMap();
    this.offloadedTasksPerStage = new ArrayList<>();
    this.messageEnvironment = messageEnvironment;
    this.stageExecutorThreadMap = stageExecutorThreadMap;
    this.executorThreads = executorThreads;
    this.evalConf = evalConf;
    this.toMaster = toMaster;
    this.scalingOutCounter = scalingOutCounter;
    this.sfTaskMetrics = sfTaskMetrics;
    this.pipeManagerWorker = pipeManagerWorker;
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

    stages.sort((s1, s2) -> -s1.compareTo(s2));
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

  private Map<String, TinyTaskWorker> getWorkerTaskMap(
    final Map<String, List<String>> offloadTasks,
    final Map<String, String> taskExecutorIdMap) {

    final Map<String, List<String>> workerTaskMap = new HashMap<>();

    offloadTasks.values().stream().flatMap(Collection::stream)
      .forEach(offloadTask -> {
        final String executorId = taskExecutorIdMap.get(offloadTask);
        if (!workerTaskMap.containsKey(executorId)) {
          workerTaskMap.put(executorId, new LinkedList<>());
        }

        workerTaskMap.get(executorId).add(offloadTask);
      });

    final Map<TinyTaskWorker, List<String>> map = new HashMap<>();
    workerTaskMap.forEach((k, v) -> {
      map.put(tinyWorkerManager
        .createVmScalingWorker(nameServerAddr, nameServerPort, k), v);
    });

    LOG.info("# of vm scaling workers: {}", map.size());

    final Map<String, TinyTaskWorker> map2 = new HashMap<>();
    map.forEach((worker, l) -> {
      l.forEach(taskId -> {
        map2.put(taskId, worker);
      });
    });

    return map2;
  }

  private synchronized void scaleOutToVms(final Map<String, List<String>> offloadTasks,
                                          final Map<String, String> taskExecutorIdMap) {
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
    //final OffloadingSerializer serializer = new OffloadingExecutorSerializer();

    remainingOffloadTasks.set(totalOffloadTasks);
    LOG.info("# of offloading tasks: {}", totalOffloadTasks);


    final int len = largestLength(stageTasks);

    final Map<String, Integer> offloadNumMap = new HashMap<>();

    // !!! Throttle start
    LOG.info("Start throttleling");
    stageExecutorThreadMap.getStageExecutorThreadMap().values().stream()
      .map(pair -> pair.right())
      .flatMap(l -> l.stream())
      .forEach(executorThread -> {
        executorThread.getThrottle().set(true);
      });

    executorThreads.getExecutorThreads().forEach(executorThread -> {
      executorThread.getThrottle().set(true);
    });


    // 2. Second, create vm workers
    final Map<String, TinyTaskWorker> taskWorkerMap =
      getWorkerTaskMap(offloadTasks, taskExecutorIdMap);

    for (int i = 0; i < len; i++) {
      for (int j = 0; j < stageTasks.size(); j++) {
        final List<TaskExecutor> te = stageTasks.get(j);

        if (te.size() > i) {
          final TaskExecutor task = te.get(i);
          final String stageId = RuntimeIdManager.getStageIdFromTaskId(task.getId());

          final List<String> stageOffloadTask = offloadTasks.getOrDefault(stageId, new ArrayList<>());

          if (stageOffloadTask.contains(task.getId())) {
            offloadedTasksPerStage.get(j).add(task);

            final int offloadNum = offloadNumMap.getOrDefault(stageId, 0);
            offloadNumMap.put(stageId, offloadNum + 1);

            final TinyTaskWorker worker = taskWorkerMap.get(task.getId());

            LOG.info("Offloading {} to {} cnt: {}, remainingOffloadTask: {}",
              task.getId(), taskExecutorIdMap.get(task.getId()), offloadNum,
              remainingOffloadTasks.getRemainingCnt());

            scalingOutCounter.counter.getAndIncrement();

            task.startOffloading(System.currentTimeMillis(), worker, (m) -> {
              LOG.info("Offloading done for {} / {}", task.getId(), taskExecutorIdMap.get(task.getId()));
              // TODO: When it is ready, send ready message
              task.callTaskOffloadingDone();
              //stageOffloadingWorkerManager.endOffloading(stageId);
            });
          }
        }
      }
    }

    LOG.info("End throttleling");
    // !!! Throttle end
    stageExecutorThreadMap.getStageExecutorThreadMap().values().stream()
      .map(pair -> pair.right())
      .flatMap(l -> l.stream())
      .forEach(executorThread -> {
        executorThread.getThrottle().set(false);
      });

    executorThreads.getExecutorThreads().forEach(executorThread -> {
      executorThread.getThrottle().set(false);
    });


    while (scalingOutCounter.counter.get() > 0) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      LOG.info("Waiting scaling out... counter {}", scalingOutCounter.counter);
    }

    toMaster.getMessageSender(SCALE_DECISION_MESSAGE_LISTENER_ID)
      .send(ControlMessage.Message.newBuilder()
        .setId(RuntimeIdManager.generateMessageId())
        .setListenerId(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID)
        .setType(ControlMessage.MessageType.LocalScalingReadyDone)
        .setLocalScalingDoneMsg(ControlMessage.LocalScalingDoneMessage.newBuilder()
          .setExecutorId(executorId)
          .setType(1)
          .build())
        .build());

    LOG.info("Scale out method done {}", offloadedTasksPerStage);
  }

  private synchronized void scaleOut(final Map<String, List<String>> offloadTasks) {
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
    //final OffloadingSerializer serializer = new OffloadingExecutorSerializer();

    remainingOffloadTasks.set(totalOffloadTasks);
    LOG.info("# of offloading tasks: {}", totalOffloadTasks);

    final int len = largestLength(stageTasks);

    final Map<String, Integer> offloadNumMap = new HashMap<>();

    // !!! Throttle start
    LOG.info("Start throttleling");
    stageExecutorThreadMap.getStageExecutorThreadMap().values().stream()
      .map(pair -> pair.right())
      .flatMap(l -> l.stream())
      .forEach(executorThread -> {
        executorThread.getThrottle().set(true);
      });

    executorThreads.getExecutorThreads().forEach(executorThread -> {
      executorThread.getThrottle().set(true);
    });

    for (int i = 0; i < len; i++) {
      for (int j = 0; j < stageTasks.size(); j++) {
        final List<TaskExecutor> te = stageTasks.get(j);

        if (te.size() > i) {
          final TaskExecutor task = te.get(i);
          final String stageId = RuntimeIdManager.getStageIdFromTaskId(task.getId());

          final List<String> stageOffloadTask = offloadTasks.getOrDefault(stageId, new ArrayList<>());

          if (stageOffloadTask.contains(task.getId())) {
            offloadedTasksPerStage.get(j).add(task);

            final int offloadNum = offloadNumMap.getOrDefault(stageId, 0);
            offloadNumMap.put(stageId, offloadNum + 1);

            final TinyTaskWorker worker;
            if (evalConf.offloadingType.equals("vm")) {
              worker = tinyWorkerManager.prepareSendTask();
            } else {
              worker = tinyWorkerManager.createWorker();
            }

            LOG.info("Offloading {}, cnt: {}, remainingOffloadTask: {}", task.getId(), offloadNum,
              remainingOffloadTasks.getRemainingCnt());

            scalingOutCounter.counter.getAndIncrement();

            task.startOffloading(System.currentTimeMillis(), worker, (m) -> {
              LOG.info("Offloading done for {}", task.getId());
              // TODO: When it is ready, send ready message
              task.callTaskOffloadingDone();
              //stageOffloadingWorkerManager.endOffloading(stageId);
            });
          }
        }
      }
    }

    LOG.info("End throttleling");
    // !!! Throttle end
    stageExecutorThreadMap.getStageExecutorThreadMap().values().stream()
      .map(pair -> pair.right())
      .flatMap(l -> l.stream())
      .forEach(executorThread -> {
        executorThread.getThrottle().set(false);
      });

    executorThreads.getExecutorThreads().forEach(executorThread -> {
      executorThread.getThrottle().set(false);
    });


    while (scalingOutCounter.counter.get() > 0) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      LOG.info("Waiting scaling out... counter {}", scalingOutCounter.counter);
    }

    toMaster.getMessageSender(SCALE_DECISION_MESSAGE_LISTENER_ID)
      .send(ControlMessage.Message.newBuilder()
        .setId(RuntimeIdManager.generateMessageId())
        .setListenerId(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID)
        .setType(ControlMessage.MessageType.LocalScalingReadyDone)
        .setLocalScalingDoneMsg(ControlMessage.LocalScalingDoneMessage.newBuilder()
          .setExecutorId(executorId)
          .setType(1)
          .build())
        .build());

    LOG.info("Scale out method done {}", offloadedTasksPerStage);
  }

  private synchronized void scaleIn() {
    // scale in
    LOG.info("Offload tasks per stage: {}", offloadedTasksPerStage);

    final CountDownLatch countDownLatch = new CountDownLatch(
      offloadedTasksPerStage.stream()
      .map(l -> l.size())
      .reduce(0, (x,y) -> x+y));

    LOG.info("Deoffloading size {}", countDownLatch.getCount());

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
          countDownLatch.countDown();
        });
      }
    }

    offloadedTasksPerStage.clear();

    // TODO: FIX
    for (final String key : taskLocationMap.locationMap.keySet()) {
      taskLocationMap.locationMap.put(key, VM);
    }

    // send done message
    LOG.info("Waiting for scaling in countdown latch");
    try {
      countDownLatch.await();

      sfTaskMetrics.sfTaskMetrics.clear();
      sfTaskMetrics.cpuLoadMap.clear();

      LOG.info("Send scaling in done");

      toMaster.getMessageSender(SCALE_DECISION_MESSAGE_LISTENER_ID)
        .send(ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID)
          .setType(ControlMessage.MessageType.LocalScalingReadyDone)
          .setLocalScalingDoneMsg(ControlMessage.LocalScalingDoneMessage.newBuilder()
            .setExecutorId(executorId)
            .setType(2)
            .build())
          .build());
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

  }

  @Override
  public void triggerPolicy() {

  }

  public void close() {
  }


  private void scaleOutWithDivideNum(final double divideNum) {
    // scale out
    final StatelessTaskStatInfo taskStatInfo = PolicyUtils.measureTaskStatInfo(taskExecutorMap);

    final List<List<TaskExecutor>> stageTasks = stageTasks(taskStatInfo.runningTasks);
    final Map<String, Integer> stageOffloadCntMap = new HashMap<>();

    int totalOffloadTasks = 0;
    for (final List<TaskExecutor> e : stageTasks) {
      final List<TaskExecutor> offloaded = new ArrayList<>();
      offloadedTasksPerStage.add(offloaded);

      final int offloadTaskNum = (e.size() - (int) (e.size() / divideNum));
      totalOffloadTasks += offloadTaskNum;
      stageOffloadCntMap.put(RuntimeIdManager.getStageIdFromTaskId(e.get(0).getId()), offloadTaskNum);
      LOG.info("Stage offloading num {}: {}",
        RuntimeIdManager.getStageIdFromTaskId(e.get(0).getId()), offloadTaskNum);
    }

    LOG.info("# of offloading tasks: {}", totalOffloadTasks);

    final int len = largestLength(stageTasks);

    // !!! Throttle start
    LOG.info("Start throttleling");
    stageExecutorThreadMap.getStageExecutorThreadMap().values().stream()
      .map(pair -> pair.right())
      .flatMap(l -> l.stream())
      .forEach(executorThread -> {
        executorThread.getThrottle().set(true);
      });

    final Map<String, Integer> offloadNumMap = new HashMap<>();
    final List<String> offloadedTasks = new ArrayList<>();

    for (int i = 0; i < len; i++) {
      for (int j = 0; j < stageTasks.size(); j++) {
        final List<TaskExecutor> te = stageTasks.get(j);

        if (te.size() > i) {
          final TaskExecutor task = te.get(i);
          final String stageId = RuntimeIdManager.getStageIdFromTaskId(task.getId());

          final int offloadcnt = offloadNumMap.getOrDefault(stageId, 0);

          if (offloadcnt < stageOffloadCntMap.get(stageId)) {
            offloadedTasks.add(task.getId());
            offloadNumMap.put(stageId, offloadcnt + 1);
            offloadedTasksPerStage.get(j).add(task);

            final TinyTaskWorker worker = tinyWorkerManager.prepareSendTask();

            LOG.info("Offloading {}, cnt: {}, ", task.getId(), offloadcnt);

            task.startOffloading(System.currentTimeMillis(), worker, (m) -> {
              LOG.info("Offloading done for {}", task.getId());
              // TODO: When it is ready, send ready message
              task.callTaskOffloadingDone();
              //stageOffloadingWorkerManager.endOffloading(stageId);
            });
          }
        }
      }
    }

    LOG.info("End throttleling");
    // !!! Throttle end
    stageExecutorThreadMap.getStageExecutorThreadMap().values().stream()
      .map(pair -> pair.right())
      .flatMap(l -> l.stream())
      .forEach(executorThread -> {
        executorThread.getThrottle().set(false);
      });
    LOG.info("Scale out method done {}", offloadedTasksPerStage);


    // send local scaling done
    scalingService.execute(() -> {
      LOG.info("Send LocalScalingDone {}", executorId);

      // Send local offloading done
      toMaster.getMessageSender(SCALE_DECISION_MESSAGE_LISTENER_ID)
        .send(ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID)
          .setType(ControlMessage.MessageType.LocalScalingReadyDone)
          .setLocalScalingDoneMsg(ControlMessage.LocalScalingDoneMessage.newBuilder()
            .setExecutorId(executorId)
            .setType(1)
            .addAllOffloadedTasks(offloadedTasks)
            .build())
          .build());

    });
  }

  private TaskLoc intToTaskLoc(final int loc) {
    if (loc == 0) {
      return VM;
    } else if (loc == 1) {
      return SF;
    } else if (loc == 2) {
      return VM_SCALING;
    } else {
      throw new RuntimeException("Invalid loc value " + loc);
    }
  }

  private final class ScalingDecisionHandler implements MessageListener<ControlMessage.Message> {

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    @Override
    public void onMessage(final ControlMessage.Message message) {
      switch (message.getType()) {
        case BroadcastInfo: {
          LOG.info("Receive info {}", message.getBroadcastInfoMsg().getInfo());
          break;
        }
        case RequestScaling:
          final ControlMessage.RequestScalingMessage scalingMsg = message.getRequestScalingMsg();

          if (scalingMsg.getIsScaleOut()) {

            if (scalingMsg.hasDivideNum()) {
              // just get the divide number and decides tasks locally

              if (!GlobalOffloadDone.getInstance().getBoolean().compareAndSet(true, false)) {
                throw new RuntimeException("GlobalOffloadDone should be true... but false TT");
              }

              final List<ControlMessage.TaskLocation> locations = scalingMsg.getTaskLocationList();
              for (final ControlMessage.TaskLocation location : locations) {
                final String taskId = location.getTaskId();
                final TaskLoc loc = intToTaskLoc((int)location.getLocation());
                taskLocationMap.locationMap.put(taskId, loc);
              }

              scaleOutWithDivideNum(scalingMsg.getDivideNum());

            } else {
              final List<ControlMessage.TaskLocation> taskLocations = scalingMsg.getTaskLocationList();
              final Map<String, String> taskExecutorIdMap = pipeManagerWorker.getTaskExecutorIdMap();

              // update task location
              taskLocationMap.locationMap.clear();
              for (final ControlMessage.TaskLocation taskLocation : taskLocations) {
                taskLocationMap.locationMap.put(taskLocation.getTaskId(), intToTaskLoc((int)taskLocation.getLocation()));
              }

              final List<ControlMessage.TaskExecutorIdEntryMessage> taskExecutorIdEntryMessages =
                scalingMsg.getTaskExecutorIdList();

              for (final ControlMessage.TaskExecutorIdEntryMessage entry : taskExecutorIdEntryMessages) {
                taskExecutorIdMap.put(entry.getTaskId(), entry.getExecutorId());
              }


              //LOG.info("Updating task location... {}", taskLocationMap.locationMap);

              final List<ControlMessage.RequestScalingEntryMessage> entries = scalingMsg.getEntryList();

              final Map<String, List<String>> scalingTaskMap = new HashMap<>();
              for (final ControlMessage.RequestScalingEntryMessage entry : entries) {
                scalingTaskMap.put(entry.getStageId(), entry.getOffloadTasksList());
              }

              LOG.info("Receive RequestScalingOut... {}", scalingTaskMap);

              /*
              if (!GlobalOffloadDone.getInstance().getBoolean().compareAndSet(true, false)) {
                throw new RuntimeException("GlobalOffloadDone should be true... but false TT");
              }
              */

              if (evalConf.offloadingType.equals("vm")) {
                // we provide task-executorId map because the task should be moved to another executor
                scaleOutToVms(scalingTaskMap, taskExecutorIdMap);
              } else {
                scaleOut(scalingTaskMap);
              }

              /*
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
              */
            }
          } else {
            // Scaling in
            LOG.info("Receive ScalingIn");
            scalingService.execute(JobScalingHandlerWorker.this::scaleIn);
          }

          break;
        case GlobalScalingReadyDone:
          LOG.info("Receive GlobalScalingReadyDone");
          //TODO: setting locations
          final ControlMessage.GlobalScalingDoneMessage msg = message.getGlobalScalingDoneMsg();
          final List<String> offloadedTasks = msg.getOffloadedTasksList();

          LOG.info("Offloaded tasks: {}", offloadedTasks);
          for (final String offloadedTask : offloadedTasks) {
            taskLocationMap.locationMap.put(offloadedTask, SF);
          }

          if (!GlobalOffloadDone.getInstance().getBoolean().compareAndSet(false, true)) {
            throw new RuntimeException("Something wrong... offloadDone should be false,, but true");
          }

          break;
        case Throttling: {
          LOG.info("Start Throttleing");

          scheduledExecutorService.schedule(() -> {
            stageExecutorThreadMap.getStageExecutorThreadMap().values().stream()
              .map(pair -> pair.right())
              .flatMap(l -> l.stream())
              .forEach(executorThread -> {
                executorThread.getThrottle().set(true);
              });
          }, 10, TimeUnit.MILLISECONDS);

          // sf worker에게도 전달.
          tinyWorkerManager.sendThrottle();

          scheduledExecutorService.schedule(() -> {
            LOG.info("End of Throttleing");
            stageExecutorThreadMap.getStageExecutorThreadMap().values().stream()
              .map(pair -> pair.right())
              .flatMap(l -> l.stream())
              .forEach(executorThread -> {
                executorThread.getThrottle().set(false);
              });
          }, 500, TimeUnit.MILLISECONDS);

          break;
        }
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
