package edu.snu.onyx.runtime.common.message.local;

import edu.snu.onyx.runtime.common.message.MessageEnvironment;
import edu.snu.onyx.runtime.common.message.MessageListener;
import edu.snu.onyx.runtime.common.message.MessageSender;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * A simple {@link MessageEnvironment} implementation that works on a single node.
 * Used for unit tests.
 */
public final class LocalMessageEnvironment implements MessageEnvironment {

  private final String currentNodeId;
  private final LocalMessageDispatcher dispatcher;

  public LocalMessageEnvironment(final String currentNodeId,
                                 final LocalMessageDispatcher dispatcher) {
    this.currentNodeId = currentNodeId;
    this.dispatcher = dispatcher;
  }

  @Override
  public <T> void setupListener(
      final String listenerId, final MessageListener<T> listener) {
    dispatcher.setupListener(currentNodeId, listenerId, listener);
  }

  @Override
  public void removeListener(final String listenerId) {
    dispatcher.removeListener(currentNodeId, listenerId);
  }

  @Override
  public <T> Future<MessageSender<T>> asyncConnect(
      final String targetId, final String messageTypeId) {
    return CompletableFuture.completedFuture(new LocalMessageSender<T>(
        currentNodeId, targetId, messageTypeId, dispatcher));
  }

  @Override
  public String getId() {
    return currentNodeId;
  }

  @Override
  public void close() {
    // No-ops.
  }
}
