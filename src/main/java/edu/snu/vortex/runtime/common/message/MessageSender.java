package edu.snu.vortex.runtime.common.message;

import java.util.concurrent.Future;

/**
 * This class sends messages to {@link MessageListener} with some defined semantics.
 * @param <T> message type
 */
public interface MessageSender<T> {

  /**
   * Send a message to corresponding {@link MessageListener#onMessage}. It does not guarantee whether
   * the message is sent successfully or not.
   *
   * @param message a message
   */
  void send(T message);

  /**
   * Send a message to corresponding {@link MessageListener#onMessageWithContext} and return
   * a reply message. If there was an exception, the returned future would be failed.
   *
   * @param message a message
   * @param <U> reply message type.
   * @return a future
   */
  <U> Future<U> request(T message);
}
