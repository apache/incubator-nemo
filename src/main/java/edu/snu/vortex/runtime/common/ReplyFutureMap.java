package edu.snu.vortex.runtime.common;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Orchestrate message sender and receiver using {@link CompletableFuture} for asynchronous request-reply communication.
 * @param <T> the type of successful reply
 */
public final class ReplyFutureMap<T> {

  private final ConcurrentHashMap<Long, CompletableFuture<T>> requestIdToFuture;

  public ReplyFutureMap() {
    requestIdToFuture = new ConcurrentHashMap<>();
  }

  /**
   * Called by message sender, just before a new request is sent.
   * Note that this method should be used *before* actual message sending.
   * Otherwise {@code onSuccessMessage} can be called before putting new future to {@code requestIdToFuture}.
   * @param id the request id
   * @return a {@link CompletableFuture} for the reply
   */
  public CompletableFuture<T> beforeRequest(final long id) {
    final CompletableFuture<T> future = new CompletableFuture<>();
    requestIdToFuture.put(id, future);
    return future;
  }

  /**
   * Called by message receiver, for a successful reply message.
   * @param id the request id
   * @param successMessage the reply message
   */
  public void onSuccessMessage(final long id, final T successMessage) {
    requestIdToFuture.remove(id).complete(successMessage);
  }

  /**
   * Called for a failure in request-reply communication.
   * @param id the request id
   * @param ex throwable exception
   */
  public void onFailure(final long id, final Throwable ex) {
    requestIdToFuture.remove(id).completeExceptionally(ex);
  }
}
