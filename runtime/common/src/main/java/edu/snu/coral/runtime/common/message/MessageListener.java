package edu.snu.coral.runtime.common.message;


/**
 * Handles messages from {@link MessageSender}. Multiple MessageListeners can be setup using {@link MessageEnvironment}
 * while they are identified by their unique message type ids.
 *
 * @param <T> message type
 */
public interface MessageListener<T> {

  /**
   * Called back when a message is received.
   * @param message a message
   */
  void onMessage(T message);

  /**
   * Called back when a message is received, and return a response using {@link MessageContext}.
   * @param message a message
   * @param messageContext a message context
   */
  void onMessageWithContext(T message, MessageContext messageContext);

}
