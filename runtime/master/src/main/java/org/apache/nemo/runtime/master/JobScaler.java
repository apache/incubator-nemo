package org.apache.nemo.runtime.master;

import org.apache.nemo.common.exception.IllegalMessageException;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.TaskLoc;
import org.apache.nemo.runtime.common.TaskLocationMap;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageContext;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.MessageListener;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.nemo.common.TaskLoc.SF;
import static org.apache.nemo.common.TaskLoc.VM;

public final class JobScaler {

  private static final Logger LOG = LoggerFactory.getLogger(JobScaler.class.getName());

  private final TaskScheduledMap taskScheduledMap;

  private Map<ExecutorRepresenter, Map<String, Integer>> prevScalingCountMap;

  private final AtomicBoolean isScaling = new AtomicBoolean(false);

  private final TaskLocationMap taskLocationMap;
  private final ExecutorService executorService;

  private final AtomicInteger scalingExecutorCnt = new AtomicInteger(0);

  @Inject
  private JobScaler(final TaskScheduledMap taskScheduledMap,
                    final MessageEnvironment messageEnvironment,
                    final TaskLocationMap taskLocationMap) {
    messageEnvironment.setupListener(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID,
      new ScaleDecisionMessageReceiver());
    this.taskScheduledMap = taskScheduledMap;
    this.prevScalingCountMap = new HashMap<>();
    this.taskLocationMap = taskLocationMap;
    this.executorService = Executors.newCachedThreadPool();
  }

  public void scalingOut(final ControlMessage.ScalingMessage msg, final boolean isNumber) {

    final double divide = msg.getDivide();
    final int prevScalingCount = sumCount();

    if (prevScalingCount > 0) {
      throw new RuntimeException("Scaling count should be equal to 0 when scaling out... but "
        + prevScalingCount + ", " + prevScalingCountMap);
    }

    if (!isScaling.compareAndSet(false, true)) {
      throw new RuntimeException("Scaling false..." + isScaling.get());
    }

    final int query = msg.hasQuery() ? msg.getQuery() : 0;

    if (isNumber) {
      scalingOutNumTasksToWorkers((int) divide);
    } else {
      scalingOutToWorkers(divide, query);
      //scalingOutToWorkerWithSimpleDecision(divide);
    }
  }

  private ControlMessage.RequestScalingMessage buildRequestScalingMessage(
    final Map<String, List<String>> offloadTaskMap,
    final List<ControlMessage.TaskLocation> locations,
    final boolean isScaleOut) {

    final ControlMessage.RequestScalingMessage.Builder builder =
    ControlMessage.RequestScalingMessage.newBuilder();

    builder.setRequestId(RuntimeIdManager.generateMessageId());
    builder.setIsScaleOut(isScaleOut);
    builder.addAllTaskLocation(locations);

    for (final Map.Entry<String, List<String>> entry : offloadTaskMap.entrySet()) {
      builder.addEntry(ControlMessage.RequestScalingEntryMessage
        .newBuilder()
        .setStageId(entry.getKey())
        .addAllOffloadTasks(entry.getValue())
        .build());
    }

    return builder.build();
  }


  private List<ControlMessage.TaskLocation> encodeTaskLocationMap() {
    final List<ControlMessage.TaskLocation> list = new ArrayList<>(taskLocationMap.locationMap.size());
    for (final Map.Entry<String, TaskLoc> entry : taskLocationMap.locationMap.entrySet()) {
      list.add(ControlMessage.TaskLocation.newBuilder()
        .setTaskId(entry.getKey())
        .setIsVm(entry.getValue() == VM)
        .build());
    }
    return list;
  }


  private void scalingOutNumTasksToWorkers(final int num) {

    // 1. update all task location
    final Map<ExecutorRepresenter, Map<String, List<String>>> workerOffloadTaskMap = new HashMap<>();

    final int offloadPerExecutor = num / taskScheduledMap.getScheduledStageTasks().keySet().size();

    int totalOffloading = 0;

    for (final ExecutorRepresenter representer :
      taskScheduledMap.getScheduledStageTasks().keySet()) {


      workerOffloadTaskMap.put(representer, new HashMap<>());

      final Map<String, List<Task>> tasks = taskScheduledMap.getScheduledStageTasks(representer);
      final Map<String, List<String>> offloadTaskMap = workerOffloadTaskMap.get(representer);

      final int numStages = tasks.size();


      for (final Map.Entry<String, List<Task>> entry : tasks.entrySet()) {
        // if query 5, do not move stage2 tasks

        /*
        if (queryNum == 5) {
          if (entry.getKey().equals("Stage3")) {
            LOG.info("Skip stage 3 offloading");
            continue;
          }
        }
        */

        final int countToOffload = offloadPerExecutor / numStages;
        final List<String> offloadTask = new ArrayList<>();
        offloadTaskMap.put(entry.getKey(), offloadTask);

        int offloadedCnt = 0;
        for (final Task task : entry.getValue()) {
          if (offloadedCnt < countToOffload) {
            final TaskLoc loc = taskLocationMap.locationMap.get(task.getTaskId());

            if (loc == VM) {
              offloadTask.add(task.getTaskId());
              taskLocationMap.locationMap.put(task.getTaskId(), SF);
              offloadedCnt += 1;
              totalOffloading += 1;
            }
          }
        }
      }
    }

    LOG.info("Total offloading cnt: {}", totalOffloading);

    final List<ControlMessage.TaskLocation> taskLocations = encodeTaskLocationMap();

    for (final Map.Entry<ExecutorRepresenter,
      Map<String, List<String>>> entry : workerOffloadTaskMap.entrySet()) {
      scalingExecutorCnt.getAndIncrement();

      final ExecutorRepresenter representer = entry.getKey();
      final Map<String, List<String>> offloadTaskMap = entry.getValue();

      // scaling out message
      LOG.info("Send scaling out message {} to {}", entry.getValue(),
        representer.getExecutorId());

      executorService.execute(() -> {
        representer.sendControlMessage(
          ControlMessage.Message.newBuilder()
            .setId(RuntimeIdManager.generateMessageId())
            .setListenerId(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID)
            .setType(ControlMessage.MessageType.RequestScaling)
            .setRequestScalingMsg(buildRequestScalingMessage(offloadTaskMap, taskLocations, true))
            .build());
      });
    }
  }

  private void scalingOutToWorkerWithSimpleDecision(final double divide) {
     // 1. update all task location
    final Map<ExecutorRepresenter, Map<String, List<String>>> workerOffloadTaskMap = new HashMap<>();

    final ControlMessage.RequestScalingMessage.Builder builder =
      ControlMessage.RequestScalingMessage.newBuilder();

    final List<ControlMessage.TaskLocation> taskLocations = encodeTaskLocationMap();

    builder.setRequestId(RuntimeIdManager.generateMessageId());
    builder.setIsScaleOut(true);
    builder.setDivideNum(divide);
    builder.addAllTaskLocation(taskLocations);

    for (final ExecutorRepresenter representer :
      taskScheduledMap.getScheduledStageTasks().keySet()) {

      scalingExecutorCnt.getAndIncrement();

      executorService.execute(() -> {
        representer.sendControlMessage(
          ControlMessage.Message.newBuilder()
            .setId(RuntimeIdManager.generateMessageId())
            .setListenerId(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID)
            .setType(ControlMessage.MessageType.RequestScaling)
            .setRequestScalingMsg(builder.build())
            .build());
      });
    }
  }


  private void scalingOutToWorkers(final double divide,
                                   final int queryNum) {

    // 1. update all task location
    final Map<ExecutorRepresenter, Map<String, List<String>>> workerOffloadTaskMap = new HashMap<>();

    for (final ExecutorRepresenter representer :
      taskScheduledMap.getScheduledStageTasks().keySet()) {

      workerOffloadTaskMap.put(representer, new HashMap<>());

      final Map<String, List<Task>> tasks = taskScheduledMap.getScheduledStageTasks(representer);
      final Map<String, List<String>> offloadTaskMap = workerOffloadTaskMap.get(representer);

      for (final Map.Entry<String, List<Task>> entry : tasks.entrySet()) {
        // if query 5, do not move stage2 tasks

        /*
        if (queryNum == 5) {
          if (entry.getKey().equals("Stage3")) {
            LOG.info("Skip stage 3 offloading");
            continue;
          }
        }
        */

        final int countToOffload = (int) (entry.getValue().size() - (entry.getValue().size() / divide));
        final List<String> offloadTask = new ArrayList<>();
        offloadTaskMap.put(entry.getKey(), offloadTask);

        int offloadedCnt = 0;
        for (final Task task : entry.getValue()) {
          if (offloadedCnt < countToOffload) {
            final TaskLoc loc = taskLocationMap.locationMap.get(task.getTaskId());

            if (loc == VM) {
              offloadTask.add(task.getTaskId());
              taskLocationMap.locationMap.put(task.getTaskId(), SF);
              offloadedCnt += 1;
            }
          }
        }
      }
    }

    final List<ControlMessage.TaskLocation> taskLocations = encodeTaskLocationMap();

    for (final Map.Entry<ExecutorRepresenter,
      Map<String, List<String>>> entry : workerOffloadTaskMap.entrySet()) {
      scalingExecutorCnt.getAndIncrement();

      final ExecutorRepresenter representer = entry.getKey();
      final Map<String, List<String>> offloadTaskMap = entry.getValue();

      // scaling out message
      LOG.info("Send scaling out message {} to {}", entry.getValue(),
        representer.getExecutorId());

      executorService.execute(() -> {
        representer.sendControlMessage(
          ControlMessage.Message.newBuilder()
            .setId(RuntimeIdManager.generateMessageId())
            .setListenerId(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID)
            .setType(ControlMessage.MessageType.RequestScaling)
            .setRequestScalingMsg(buildRequestScalingMessage(offloadTaskMap, taskLocations, true))
            .build());
      });
    }
  }

  private int sumCount() {
    return prevScalingCountMap.values().stream()
      .map(m -> m.values().stream().reduce(0, (x,y) -> x+y)) .reduce(0, (x, y) -> x+y);
  }

  public void scalingIn() {

    final Map<String, List<String>> unloadTaskMap = new HashMap<>();

    for (final String key : taskLocationMap.locationMap.keySet()) {
      if (taskLocationMap.locationMap.get(key) == SF) {
        final String stageId = RuntimeIdManager.getStageIdFromTaskId(key);
        unloadTaskMap.putIfAbsent(stageId, new ArrayList<>());
        final List<String> unloadTasks = unloadTaskMap.get(stageId);
        unloadTasks.add(key);
        taskLocationMap.locationMap.put(key, VM);
      }
    }

    final List<ControlMessage.TaskLocation> locations = encodeTaskLocationMap();

    for (final ExecutorRepresenter representer :
      taskScheduledMap.getScheduledStageTasks().keySet()) {
      executorService.execute(() -> {
        representer.sendControlMessage(
          ControlMessage.Message.newBuilder()
            .setId(RuntimeIdManager.generateMessageId())
            .setListenerId(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID)
            .setType(ControlMessage.MessageType.RequestScaling)
            .setRequestScalingMsg(buildRequestScalingMessage(unloadTaskMap, locations, false))
            .build());
      });
    }
  }

  private void sendScalingOutDoneToAllWorkers() {
    LOG.info("Send scale out done to all workers");

    final List<String> offloadedTasks = taskLocationMap.locationMap.entrySet()
      .stream().filter(entry -> entry.getValue().equals(SF))
      .map(entry -> entry.getKey())
      .collect(Collectors.toList());

    LOG.info("Offloaded tasks: {}", offloadedTasks);

    for (final ExecutorRepresenter representer : taskScheduledMap.getScheduledStageTasks().keySet()) {
      final long id = RuntimeIdManager.generateMessageId();
      executorService.execute(() -> {
      representer.sendControlMessage(ControlMessage.Message.newBuilder()
        .setId(id)
        .setListenerId(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID)
        .setType(ControlMessage.MessageType.GlobalScalingReadyDone)
        .setGlobalScalingDoneMsg(ControlMessage.GlobalScalingDoneMessage.newBuilder()
          .setRequestId(id)
          .addAllOffloadedTasks(offloadedTasks)
          .build())
        .build());
      });

    }
  }

  public final class ScaleDecisionMessageReceiver implements MessageListener<ControlMessage.Message> {
    @Override
    public void onMessage(final ControlMessage.Message message) {
      switch (message.getType()) {
        case LocalScalingReadyDone:
          final ControlMessage.LocalScalingDoneMessage localScalingDoneMessage = message.getLocalScalingDoneMsg();
          final String executorId = localScalingDoneMessage.getExecutorId();

          final ExecutorRepresenter executorRepresenter = taskScheduledMap.getExecutorRepresenter(executorId);
          LOG.info("Receive LocalScalingDone for {}", executorId);

          for (final String offloadedTask : localScalingDoneMessage.getOffloadedTasksList()) {
            taskLocationMap.locationMap.put(offloadedTask, SF);
          }

          final int cnt = scalingExecutorCnt.decrementAndGet();
          if (cnt == 0) {
            if (isScaling.compareAndSet(true, false)) {
              sendScalingOutDoneToAllWorkers();
            }
          }

          break;
        default:
          throw new IllegalMessageException(new Exception(message.toString()));
      }
    }

    @Override
    public void onMessageWithContext(ControlMessage.Message message, MessageContext messageContext) {
      throw new RuntimeException("Not supported");
    }
  }
}
