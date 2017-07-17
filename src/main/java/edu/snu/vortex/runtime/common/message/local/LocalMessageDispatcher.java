package edu.snu.vortex.runtime.common.message.local;

import edu.snu.vortex.runtime.common.message.MessageListener;
import edu.snu.vortex.runtime.common.message.MessageSender;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Dispatch messages on a single machine.
 */
public final class LocalMessageDispatcher {

  private final ConcurrentMap<String, ConcurrentMap<String, MessageListener>> nodeIdToMessageListenersMap;

  public LocalMessageDispatcher() {
    this.nodeIdToMessageListenersMap = new ConcurrentHashMap<>();
  }

  <T> MessageSender<T> setupListener(
      final String currentNodeId, final String messageTypeId, final MessageListener<T> listener) {

    ConcurrentMap<String, MessageListener> messageTypeToListenerMap = nodeIdToMessageListenersMap.get(currentNodeId);

    if (messageTypeToListenerMap == null) {
      messageTypeToListenerMap = new ConcurrentHashMap<>();
      final ConcurrentMap<String, MessageListener> map = nodeIdToMessageListenersMap.putIfAbsent(
          currentNodeId, messageTypeToListenerMap);
      if (map != null) {
        messageTypeToListenerMap = map;
      }
    }

    if (messageTypeToListenerMap.putIfAbsent(messageTypeId, listener) != null) {
      throw new LocalDispatcherException(
          messageTypeId + " was already used in " + currentNodeId);
    }

    return new LocalMessageSender<>(currentNodeId, currentNodeId, messageTypeId, this);
  }

  <T> void dispatchSendMessage(
      final String targetId, final String messageTypeId, final T message) {
    final MessageListener listener = nodeIdToMessageListenersMap.get(targetId).get(messageTypeId);
    if (listener == null) {
      throw new LocalDispatcherException("There was no set up listener for " + messageTypeId + " in " + targetId);
    }
    listener.onMessage(message);
  }

  <T, U> CompletableFuture<U> dispatchRequestMessage(
      final String senderId, final String targetId, final String messageTypeId, final T message) {

    final MessageListener listener = nodeIdToMessageListenersMap.get(targetId).get(messageTypeId);
    if (listener == null) {
      throw new LocalDispatcherException("There was no set up listener for " + messageTypeId + " in " + targetId);
    }

    final LocalMessageContext context = new LocalMessageContext(senderId);
    listener.onMessageWithContext(message, context);

    final Optional<Object> replyMessage = context.getReplyMessage();

    return CompletableFuture.completedFuture((U) replyMessage.orElse(null));
  }

  /**
   * A runtime exception in {@link LocalMessageDispatcher}.
   */
  private final class LocalDispatcherException extends RuntimeException {
    LocalDispatcherException(final String message) {
      super(message);
    }
  }
}
