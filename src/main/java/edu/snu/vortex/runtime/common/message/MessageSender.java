package edu.snu.vortex.runtime.common.message;

import java.io.Serializable;
import java.util.concurrent.Future;

/**
 * This class sends messages to {@link MessageListener} with some defined semantics.
 * @param <T> message type
 */
public interface MessageSender<T extends Serializable> {

  /**
   * Send a message to corresponding {@link MessageListener#onSendMessage(Serializable)}. It does not guarantee whether
   * the message is sent successfully or not.
   *
   * @param message a message
   */
  void send(T message);

  /**
   * Send a message to corresponding {@link MessageListener#onRequestMessage(Serializable, MessageContext)} and return
   * a reply message. If there was an exception, the returned future would be failed.
   *
   * @param message a message
   * @param <U> reply message type.
   * @return a future
   */
  <U extends Serializable> Future<U> request(T message);

}
