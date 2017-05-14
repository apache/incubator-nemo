package edu.snu.vortex.runtime.common.message.local;

import edu.snu.vortex.runtime.common.message.MessageSender;

import java.util.concurrent.Future;

/**
 * A simple {@link MessageSender} implementation that works on a single node.
 * @param <T> a message type
 */
public final class LocalMessageSender<T> implements MessageSender<T> {

  private final String senderId;
  private final String targetId;
  private final String messageTypeId;
  private final LocalMessageDispatcher dispatcher;

  public LocalMessageSender(final String senderId,
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
  public <U> Future<U> request(final T message) {
    return dispatcher.dispatchRequestMessage(senderId, targetId, messageTypeId, message);
  }
}
