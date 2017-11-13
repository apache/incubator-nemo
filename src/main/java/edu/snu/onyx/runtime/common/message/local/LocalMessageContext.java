package edu.snu.onyx.runtime.common.message.local;

import edu.snu.onyx.runtime.common.message.MessageContext;

import java.util.Optional;

/**
 * A simple {@link MessageContext} implementation that works on a single node.
 */
final class LocalMessageContext implements MessageContext {

  private final String senderId;
  private Object replyMessage;

  /**
   *  TODO #119.
   * @param senderId  TODO #119.
   */
  LocalMessageContext(final String senderId) {
    this.senderId = senderId;
  }

  public String getSenderId() {
    return senderId;
  }

  @Override
  public <T> void reply(final T message) {
    this.replyMessage = message;
  }

  /**
   *  TODO #119.
   * @return TODO #119.
   */
  public Optional<Object> getReplyMessage() {
    return Optional.ofNullable(replyMessage);
  }
}
