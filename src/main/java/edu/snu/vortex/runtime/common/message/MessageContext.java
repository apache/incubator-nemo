package edu.snu.vortex.runtime.common.message;

import java.io.Serializable;

/**
 * This class sends a reply message from {@link MessageListener}.
 */
public interface MessageContext {

  /**
   * Return the identifier of sender.
   *
   * @return sender id
   */
  String getSenderId();

  /**
   * Send back a reply message.
   *
   * @param replyMessage a reply message
   * @param <U> type of the reply message
   */
  <U extends Serializable> void reply(U replyMessage);

  /**
   * Send back a throwable.
   * @param throwable a throwable
   */
  void replyThrowable(Throwable throwable);

}
