package org.apache.nemo.runtime.common.message.local;

import org.apache.nemo.runtime.common.message.MessageContext;

import java.util.Optional;

/**
 * A simple {@link MessageContext} implementation that works on a single node.
 */
final class LocalMessageContext implements MessageContext {

  private final String senderId;
  private Object replyMessage;

  /**
   *  TODO #10: Handle Method Javadocs Requirements for Checkstyle Warnings.
   * @param senderId  TODO #10: Handle Method Javadocs Requirements for Checkstyle Warnings.
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
   *  TODO #10: Handle Method Javadocs Requirements for Checkstyle Warnings.
   * @return TODO #10: Handle Method Javadocs Requirements for Checkstyle Warnings.
   */
  public Optional<Object> getReplyMessage() {
    return Optional.ofNullable(replyMessage);
  }
}
