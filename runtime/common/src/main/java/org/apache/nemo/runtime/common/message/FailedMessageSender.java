package org.apache.nemo.runtime.common.message;

import org.apache.nemo.runtime.common.comm.ControlMessage;

import java.util.concurrent.CompletableFuture;

/**
 * A message sender that failed.
 */
public final class FailedMessageSender implements MessageSender<ControlMessage.Message> {
  @Override
  public void send(final ControlMessage.Message message) {
    // Do nothing.
  }

  @Override
  public CompletableFuture<ControlMessage.Message> request(final ControlMessage.Message message) {
    final CompletableFuture<ControlMessage.Message> failed = new CompletableFuture<>();
    failed.completeExceptionally(new Throwable("Failed Message Sender"));
    return failed;
  }

  @Override
  public void close() throws Exception {
    // Do nothing.
  }
}
