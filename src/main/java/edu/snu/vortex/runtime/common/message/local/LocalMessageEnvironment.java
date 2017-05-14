package edu.snu.vortex.runtime.common.message.local;

import edu.snu.vortex.runtime.common.message.MessageEnvironment;
import edu.snu.vortex.runtime.common.message.MessageListener;
import edu.snu.vortex.runtime.common.message.MessageSender;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * A simple {@link MessageEnvironment} implementation that works on a single node.
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
  public <T> MessageSender<T> setupListener(
      final String messageTypeId, final MessageListener<T> listener) {
    return dispatcher.setupListener(currentNodeId, messageTypeId, listener);
  }

  @Override
  public <T> Future<MessageSender<T>> asyncConnect(
      final String targetId, final String messageTypeId) {
    return CompletableFuture.completedFuture(new LocalMessageSender<T>(
        currentNodeId, targetId, messageTypeId, dispatcher));
  }

  @Override
  public String getCurrentId() {
    return currentNodeId;
  }
}
