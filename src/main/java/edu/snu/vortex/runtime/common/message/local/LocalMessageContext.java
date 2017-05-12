package edu.snu.vortex.runtime.common.message.local;

import edu.snu.vortex.runtime.common.message.MessageContext;

import java.io.Serializable;
import java.util.Optional;

/**
 * A simple {@link MessageContext} implementation that works on a single node.
 */
final class LocalMessageContext implements MessageContext {

  private final String senderId;
  private Throwable throwable;
  private Object replyMessage;

  /**
   *  TODO #119.
   * @param senderId  TODO #119.
   */
  LocalMessageContext(final String senderId) {
    this.senderId = senderId;
  }

  @Override
  public String getSenderId() {
    return senderId;
  }

  @Override
  public <T extends Serializable> void reply(final T message) {
    this.replyMessage = message;
  }

  @Override
  public void replyThrowable(final Throwable th) {
    this.throwable = th;
  }

  /**
   *  TODO #119.
   * @return TODO #119.
   */
  public Optional<Throwable> getThrowable() {
    return Optional.ofNullable(throwable);
  }

  /**
   *  TODO #119.
   * @return TODO #119.
   */
  public Optional<Object> getReplyMessage() {
    return Optional.ofNullable(replyMessage);
  }
}
