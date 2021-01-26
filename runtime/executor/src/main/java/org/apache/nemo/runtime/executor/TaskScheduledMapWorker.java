package org.apache.nemo.runtime.executor;

import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageContext;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.MessageListener;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;


public final class TaskScheduledMapWorker {
  private static final Logger LOG = LoggerFactory.getLogger(TaskScheduledMapWorker.class.getName());

  // key: task id, value: executpr od
  private final Map<String, String> map = new ConcurrentHashMap<>();
  private final PersistentConnectionToMasterMap toMaster;

  @Inject
  private TaskScheduledMapWorker(
    final PersistentConnectionToMasterMap persistentConnectionToMasterMap,
    final MessageEnvironment messageEnvironment) {
    this.toMaster = persistentConnectionToMasterMap;

    messageEnvironment.setupListener(MessageEnvironment.TASK_SCHEDULE_MAP_LISTENER_ID,
      new Receiver());
  }

  public void init() {
    try {
      final CompletableFuture<ControlMessage.Message> future = toMaster
        .getMessageSender(MessageEnvironment.TASK_SCHEDULE_MAP_LISTENER_ID)
        .request(ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(MessageEnvironment.TASK_SCHEDULE_MAP_LISTENER_ID)
          .setType(ControlMessage.MessageType.CurrentScheduledTask)
          .build());

      LOG.info("Initializing executor connection...");

      final ControlMessage.Message msg = future.get();
      final List<String> scheduledTasks = msg.getCurrScheduledTasksList();
      for (final String scheduledTask : scheduledTasks) {
        final String[] split = scheduledTask.split(",");
        LOG.info("Task {} in executor {}", split[0], split[1]);
        map.put(split[0], split[1]);
      }

    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public void registerTask(final String taskId, final String executorId) {
    map.put(taskId, executorId);
  }

  public String getRemoteExecutorId(final String dstTaskId) {
    return map.get(dstTaskId);
  }

  final class Receiver implements MessageListener<ControlMessage.Message> {
    @Override
    public void onMessage(ControlMessage.Message message) {
      switch (message.getType()) {
        case TaskScheduled: {
          LOG.info("Task scheduled {}", message.getRegisteredExecutor());
          final String[] split = message.getRegisteredExecutor().split(",");
          registerTask(split[0], split[1]);
          break;
        }
        default:
          throw new RuntimeException("Invalid message type " + message.getType());
      }
    }

    @Override
    public void onMessageWithContext(ControlMessage.Message message, MessageContext messageContext) {
      throw new RuntimeException("Invalid message type " + message.getType());
    }
  }
}
