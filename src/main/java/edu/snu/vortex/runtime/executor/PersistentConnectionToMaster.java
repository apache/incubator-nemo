package edu.snu.vortex.runtime.executor;

import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.common.message.MessageEnvironment;
import edu.snu.vortex.runtime.common.message.MessageSender;
import edu.snu.vortex.runtime.exception.NodeConnectionException;

import javax.inject.Inject;
import java.util.concurrent.ExecutionException;

/**
 * Persistent connection for sending messages to master.
 */
public final class PersistentConnectionToMaster {
  private final MessageSender<ControlMessage.Message> messageSender;

  @Inject
  public PersistentConnectionToMaster(final MessageEnvironment messageEnvironment) {
    try {
      messageSender =
          messageEnvironment.<ControlMessage.Message>asyncConnect(MessageEnvironment.MASTER_COMMUNICATION_ID,
              MessageEnvironment.MASTER_MESSAGE_RECEIVER).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new NodeConnectionException(e);
    }
  }

  public MessageSender<ControlMessage.Message> getMessageSender() {
    return messageSender;
  }
}
