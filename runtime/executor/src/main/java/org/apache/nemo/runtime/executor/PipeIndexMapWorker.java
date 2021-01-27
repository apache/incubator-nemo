package org.apache.nemo.runtime.executor;

import org.apache.commons.lang3.tuple.Triple;
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

public final class PipeIndexMapWorker {
  // key: (runtimeEdgeId, dstTaskIndex), value: input contexts of this task
  private static final Logger LOG = LoggerFactory.getLogger(PipeIndexMapWorker.class.getName());

  private final ConcurrentMap<Triple<String, String, String>, Integer> map;
  private final PersistentConnectionToMasterMap toMaster;

  @Inject
  private PipeIndexMapWorker(final PersistentConnectionToMasterMap persistentConnectionToMasterMap) {
    this.toMaster = persistentConnectionToMasterMap;
    this.map = new ConcurrentHashMap<>();

    // messageEnvironment.setupListener(MessageEnvironment.TASK_INDEX_MESSAGE_LISTENER_ID,
    //  new Receiver());
  }

  public int getPipeIndex(final String srcTaskId,
                          final String edgeId,
                          final String dstTaskId) {
    final Triple<String, String, String> key = Triple.of(srcTaskId, edgeId, dstTaskId);

    if (!map.containsKey(key)) {
      final CompletableFuture<ControlMessage.Message> future = toMaster
        .getMessageSender(MessageEnvironment.TASK_INDEX_MESSAGE_LISTENER_ID)
        .request(ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(MessageEnvironment.TASK_INDEX_MESSAGE_LISTENER_ID)
          .setType(ControlMessage.MessageType.RequestTaskIndex)
          .setRequestTaskIndexMsg(ControlMessage.RequestTaskIndexMessage
            .newBuilder()
            .setSrcTaskId(srcTaskId)
            .setEdgeId(edgeId)
            .setDstTaskId(dstTaskId)
            .build())
          .build());

      try {
        final ControlMessage.Message msg = future.get();
        final ControlMessage.TaskIndexInfoMessage m = msg.getTaskIndexInfoMsg();
        map.put(key, (int) m.getTaskIndex());
        return map.get(key);
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

    return map.get(key);
  }
}