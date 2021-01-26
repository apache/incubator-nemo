package org.apache.nemo.runtime.executor;

import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class TaskIndexMapWorker {
  // key: (runtimeEdgeId, dstTaskIndex), value: input contexts of this task
  private static final Logger LOG = LoggerFactory.getLogger(TaskIndexMapWorker.class.getName());

  private final ConcurrentMap<String, Integer> map;
  private final PersistentConnectionToMasterMap toMaster;

  @Inject
  private TaskIndexMapWorker(final PersistentConnectionToMasterMap persistentConnectionToMasterMap) {
    this.toMaster = persistentConnectionToMasterMap;
    this.map = new ConcurrentHashMap<>();

    // messageEnvironment.setupListener(MessageEnvironment.TASK_INDEX_MESSAGE_LISTENER_ID,
    //  new Receiver());
  }

  public int getTaskIndex(final String taskId) {
    if (!map.containsKey(taskId)) {
      final CompletableFuture<ControlMessage.Message> future = toMaster
        .getMessageSender(MessageEnvironment.TASK_INDEX_MESSAGE_LISTENER_ID)
        .request(ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(MessageEnvironment.TASK_INDEX_MESSAGE_LISTENER_ID)
          .setType(ControlMessage.MessageType.RequestTaskIndex)
          .setRequestTaskIndexMsg(ControlMessage.RequestTaskIndexMessage
            .newBuilder()
            .setExecutorId("none")
            .setTaskId(taskId)
            .build())
          .build());

      try {
        final ControlMessage.Message msg = future.get();
        final ControlMessage.TaskIndexInfoMessage m = msg.getTaskIndexInfoMsg();
        map.put(taskId, (int) m.getTaskIndex());
        return map.get(taskId);
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

    return map.get(taskId);
  }
}
