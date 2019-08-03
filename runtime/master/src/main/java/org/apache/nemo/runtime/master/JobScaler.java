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
import java.util.concurrent.atomic.AtomicBoolean;

public final class JobScaler {

  private static final Logger LOG = LoggerFactory.getLogger(JobScaler.class.getName());

  private final TaskScheduledMap taskScheduledMap;

  private Map<ExecutorRepresenter, Map<String, Integer>> prevScalingCountMap;

  private final AtomicBoolean isScaling = new AtomicBoolean(false);

  private final TaskLocationMap taskLocationMap;

  @Inject
  private JobScaler(final TaskScheduledMap taskScheduledMap,
                    final MessageEnvironment messageEnvironment,
                    final TaskLocationMap taskLocationMap) {
    messageEnvironment.setupListener(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID,
      new ScaleDecisionMessageReceiver());
    this.taskScheduledMap = taskScheduledMap;
    this.prevScalingCountMap = new HashMap<>();
    this.taskLocationMap = taskLocationMap;
  }

  public void scalingOut(final int divide) {

    final int prevScalingCount = sumCount();

    if (prevScalingCount > 0) {
      throw new RuntimeException("Scaling count should be equal to 0 when scaling out... but "
        + prevScalingCount + ", " + prevScalingCountMap);
    }

    if (!isScaling.compareAndSet(false, true)) {
      throw new RuntimeException("Scaling false..." + isScaling.get());
    }

    scalingOutToWorkers(divide);
  }

  private ControlMessage.RequestScalingOutMessage buildRequestScalingoutMessage(
    final Map<String, List<String>> offloadTaskMap) {

    final ControlMessage.RequestScalingOutMessage.Builder builder =
    ControlMessage.RequestScalingOutMessage.newBuilder();

    builder.setRequestId(RuntimeIdManager.generateMessageId());

    // add task location!!
    for (final Map.Entry<String, TaskLoc> taskLoc : taskLocationMap.locationMap.entrySet()) {
      builder.addTaskLocation(ControlMessage.TaskLocation.newBuilder()
        .setTaskId(taskLoc.getKey())
        .setIsVm(taskLoc.getValue() == TaskLoc.VM)
        .build());
    }

    for (final Map.Entry<String, List<String>> entry : offloadTaskMap.entrySet()) {
      builder.addEntry(ControlMessage.RequestScalingEntryMessage
        .newBuilder()
        .setStageId(entry.getKey())
        .addAllOffloadTasks(entry.getValue())
        .build());
    }

    return builder.build();
  }

  private void scalingOutToWorkers(final int divide) {

    // 1. update all task location
    final Map<ExecutorRepresenter, Map<String, List<String>>> workerOffloadTaskMap = new HashMap<>();

    for (final ExecutorRepresenter representer :
      taskScheduledMap.getScheduledStageTasks().keySet()) {

      workerOffloadTaskMap.put(representer, new HashMap<>());


      final Map<String, List<Task>> tasks = taskScheduledMap.getScheduledStageTasks(representer);
      final Map<String, List<String>> offloadTaskMap = workerOffloadTaskMap.get(representer);

      for (final Map.Entry<String, List<Task>> entry : tasks.entrySet()) {
        final int countToOffload = entry.getValue().size() - (entry.getValue().size() / divide);
        final List<String> offloadTask = new ArrayList<>();
        offloadTaskMap.put(entry.getKey(), offloadTask);

        int offloadedCnt = 0;
        for (final Task task : entry.getValue()) {
          if (offloadedCnt < countToOffload) {
            final TaskLoc loc = taskLocationMap.locationMap.get(task.getTaskId());

            if (loc == TaskLoc.VM) {
              offloadTask.add(task.getTaskId());
              taskLocationMap.locationMap.put(task.getTaskId(), TaskLoc.SF);
              offloadedCnt += 1;
            }
          }
        }
      }
    }

    for (final Map.Entry<ExecutorRepresenter,
      Map<String, List<String>>> entry : workerOffloadTaskMap.entrySet()) {

      final ExecutorRepresenter representer = entry.getKey();
      final Map<String, List<String>> offloadTaskMap = entry.getValue();

      // scaling out message
      LOG.info("Send scaling out message {} to {}", entry.getValue(),
        representer.getExecutorId());

      representer.sendControlMessage(
        ControlMessage.Message.newBuilder()
        .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID)
          .setType(ControlMessage.MessageType.RequestScalingOut)
          .setRequestScalingOutMsg(buildRequestScalingoutMessage(offloadTaskMap))
          .build());
    }
  }

  private int sumCount() {
    return prevScalingCountMap.values().stream()
      .map(m -> m.values().stream().reduce(0, (x,y) -> x+y)) .reduce(0, (x, y) -> x+y);
  }

  public void scalingIn() {
    for (final ExecutorRepresenter representer :
      taskScheduledMap.getScheduledStageTasks().keySet()) {
      representer.sendControlMessage(
        ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID)
          .setType(ControlMessage.MessageType.RequestScalingIn)
          .build());
    }
  }

  private void sendScalingOutDoneToAllWorkers() {
    LOG.info("Send scale out done to all workers");

    for (final ExecutorRepresenter representer : taskScheduledMap.getScheduledStageTasks().keySet()) {
      final long id = RuntimeIdManager.generateMessageId();
      representer.sendControlMessage(ControlMessage.Message.newBuilder()
        .setId(id)
        .setListenerId(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID)
        .setType(ControlMessage.MessageType.GlobalScalingReadyDone)
        .setGlobalScalingDoneMsg(ControlMessage.GlobalScalingDoneMessage.newBuilder()
          .setRequestId(id)
          .build())
        .build());
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

          synchronized (prevScalingCountMap) {
            final Map<String, Integer> countMap = prevScalingCountMap.remove(executorRepresenter);
            LOG.info("Receive LocalScalingDone for {}, countMap {}", executorId,
              countMap);

            if (sumCount() == 0) {
              if (isScaling.compareAndSet(true, false)) {
                sendScalingOutDoneToAllWorkers();
              }
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
