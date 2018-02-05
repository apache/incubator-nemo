package edu.snu.coral.runtime.common.message;

import edu.snu.coral.runtime.common.comm.ControlMessage;
import edu.snu.coral.common.exception.NodeConnectionException;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Persistent connection for sending messages to master.
 */
public final class PersistentConnectionToMasterMap {
  private final Map<String, MessageSender<ControlMessage.Message>> messageSenders;
  private final MessageEnvironment messageEnvironment;

  @Inject
  public PersistentConnectionToMasterMap(final MessageEnvironment messageEnvironment) {
    this.messageEnvironment = messageEnvironment;
    messageSenders = new HashMap<>();
    try {
      // Connect the globally known message listener IDs.
      messageSenders.put(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID,
          messageEnvironment.<ControlMessage.Message>asyncConnect(MessageEnvironment.MASTER_COMMUNICATION_ID,
              MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID).get());
      messageSenders.put(MessageEnvironment.BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID,
          messageEnvironment.<ControlMessage.Message>asyncConnect(MessageEnvironment.MASTER_COMMUNICATION_ID,
              MessageEnvironment.BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID).get());
    } catch (InterruptedException | ExecutionException e) {
      throw new NodeConnectionException(e);
    }
  }

  /**
   * Get the message sender corresponding to the given listener ID.
   *
   * @param listenerId the ID of the listener.
   * @return the message sender.
   */
  public synchronized MessageSender<ControlMessage.Message> getMessageSender(final String listenerId) {
    final MessageSender<ControlMessage.Message> messageSender = messageSenders.get(listenerId);
    if (messageSender != null) {
      return messageSender;
    } else { // Unknown message listener.
      final MessageSender<ControlMessage.Message> createdMessageSender;
      try {
        createdMessageSender = messageEnvironment.<ControlMessage.Message>asyncConnect(
            MessageEnvironment.MASTER_COMMUNICATION_ID, listenerId).get();
        messageSenders.put(listenerId, createdMessageSender);
      } catch (InterruptedException | ExecutionException e) {
        throw new NodeConnectionException(e);
      }
      return createdMessageSender;
    }
  }
}
