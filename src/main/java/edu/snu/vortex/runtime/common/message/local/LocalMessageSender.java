package edu.snu.vortex.runtime.common.message.local;

import edu.snu.vortex.runtime.common.message.MessageSender;

import java.io.Serializable;
import java.util.concurrent.Future;

/**
 * A simple {@link MessageSender} implementation that works on a single node.
 * @param <T> a message type
 */
final class LocalMessageSender<T extends Serializable> implements MessageSender<T> {

  private final String senderId;
  private final String targetId;
  private final String messageTypeId;
  private final LocalMessageDispatcher dispatcher;

  LocalMessageSender(final String senderId,
                     final String targetId,
                     final String messageTypeId,
                     final LocalMessageDispatcher dispatcher) {
    this.senderId = senderId;
    this.targetId = targetId;
    this.messageTypeId = messageTypeId;
    this.dispatcher = dispatcher;
  }

  @Override
  public void send(final T message) {
    dispatcher.dispatchSendMessage(targetId, messageTypeId, message);
  }

  @Override
  public <U extends Serializable> Future<U> request(final T message) {
    return dispatcher.dispatchRequestMessage(senderId, targetId, messageTypeId, message);
  }
}
