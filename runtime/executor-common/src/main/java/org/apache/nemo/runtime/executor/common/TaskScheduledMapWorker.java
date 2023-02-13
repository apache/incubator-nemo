package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.message.comm.ControlMessage;
import org.apache.nemo.runtime.message.PersistentConnectionToMasterMap;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.nemo.runtime.message.MessageEnvironment.ListenerType.TASK_SCHEDULE_MAP_LISTENER_ID;


public final class TaskScheduledMapWorker {
  private static final Logger LOG = LoggerFactory.getLogger(TaskScheduledMapWorker.class.getName());

  // key: task id, value: executpr od
  private final Map<String, String> map = new ConcurrentHashMap<>();
  private final PersistentConnectionToMasterMap toMaster;
  private final String executorId;

  @Inject
  private TaskScheduledMapWorker(
    @Parameter(JobConf.ExecutorId.class) final String executorId,
    final PersistentConnectionToMasterMap persistentConnectionToMasterMap) {
    this.executorId = executorId;
    this.toMaster = persistentConnectionToMasterMap;
  }

  public void init() {
    try {
      final CompletableFuture<ControlMessage.Message> future = toMaster
        .getMessageSender(TASK_SCHEDULE_MAP_LISTENER_ID)
        .request(ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(TASK_SCHEDULE_MAP_LISTENER_ID.ordinal())
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

  public String getRemoteExecutorId(final String dstTaskId,
                                    final boolean syncMaster) {

    // return map.get(dstTaskId);
    if (syncMaster) {
      final CompletableFuture<ControlMessage.Message> future = toMaster
        .getMessageSender(TASK_SCHEDULE_MAP_LISTENER_ID)
        .request(ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(TASK_SCHEDULE_MAP_LISTENER_ID.ordinal())
          .setType(ControlMessage.MessageType.TaskScheduled)
          .setRegisteredExecutor(dstTaskId)
          .build());

      // LOG.info("Request location of {}", dstTaskId);

      final ControlMessage.Message msg;
      try {
        msg = future.get();
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

      // LOG.info("Response location of {}, {}/{}", msg.getId(),
      //  dstTaskId, msg.getRegisteredExecutor());

      final String[] split = msg.getRegisteredExecutor().split(",");

      if (split[1].equals("null")) {
        return null;
      }

      map.put(split[0], split[1]);
      return map.get(dstTaskId);
    } else {
      return map.get(dstTaskId);
    }
  }
}
