package edu.snu.vortex.runtime.common.message.local;

import edu.snu.vortex.runtime.common.message.MessageListener;
import edu.snu.vortex.runtime.common.message.MessageSender;

import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

/**
 * Dispatch messages on a single machine.
 */
final class LocalMessageDispatcher {

  private final ConcurrentMap<String, ConcurrentMap<String, MessageListener>> nodeIdToMessageListenersMap;

  LocalMessageDispatcher() {
    this.nodeIdToMessageListenersMap = new ConcurrentHashMap<>();
  }

  <T extends Serializable> MessageSender<T> setupListener(
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

  <T extends Serializable> void dispatchSendMessage(
      final String targetId, final String messageTypeId, final T message) {
    final MessageListener listener = nodeIdToMessageListenersMap.get(targetId).get(messageTypeId);
    if (listener == null) {
      throw new LocalDispatcherException("There was no set up listener for " + messageTypeId + " in " + targetId);
    }
    listener.onSendMessage(message);
  }

  <T extends Serializable, U extends Serializable> Future<U> dispatchRequestMessage(
      final String senderId, final String targetId, final String messageTypeId, final T message) {

    final MessageListener listener = nodeIdToMessageListenersMap.get(targetId).get(messageTypeId);
    if (listener == null) {
      throw new LocalDispatcherException("There was no set up listener for " + messageTypeId + " in " + targetId);
    }

    final LocalMessageContext context = new LocalMessageContext(senderId);
    listener.onRequestMessage(message, context);

    final Optional<Throwable> throwable = context.getThrowable();
    final Optional<Object> replyMessage = context.getReplyMessage();

    final CompletableFuture future = new CompletableFuture();
    if (throwable.isPresent()) {
      future.completeExceptionally(throwable.get());
    } else {
      future.complete(replyMessage.orElse(null));
    }

    return future;
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
