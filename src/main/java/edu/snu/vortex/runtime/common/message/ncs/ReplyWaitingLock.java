package edu.snu.vortex.runtime.common.message.ncs;

import edu.snu.vortex.runtime.common.comm.ControlMessage;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Lock used for NCS messages.
 */
class ReplyWaitingLock {

  private final Lock lock;
  private final Condition messageArrived;
  private final Map<Long, Future<ControlMessage.Message>> repliedFutureMap;

  @Inject
  ReplyWaitingLock() {
    this.lock = new ReentrantLock();
    this.messageArrived = this.lock.newCondition();
    this.repliedFutureMap = new HashMap<>();
  }

  Future<ControlMessage.Message> waitingReply(final long id) {
    lock.lock();
    try {
      while (!repliedFutureMap.containsKey(id)) {
        messageArrived.await();
      }

      return repliedFutureMap.remove(id);
    } catch (final InterruptedException e) {
      throw new RuntimeException("Thread is interrupted in ReplyWaitingLock", e);
    } finally {
      lock.unlock();
    }
  }

  void onSuccessMessage(final long id, final ControlMessage.Message successMessage) {
    final CompletableFuture<ControlMessage.Message> future = CompletableFuture.completedFuture(successMessage);
    lock.lock();
    try {
      repliedFutureMap.put(id, future);
      messageArrived.signalAll();
    } finally {
      lock.unlock();
    }
  }

  void onFailedMessage(final ControlMessage.Message failedMessage, final Throwable ex) {
    final CompletableFuture<ControlMessage.Message> future = new CompletableFuture<>();
    future.completeExceptionally(ex);
    lock.lock();
    try {
      repliedFutureMap.put(failedMessage.getId(), future);
      messageArrived.signalAll();
    } finally {
      lock.unlock();
    }
  }
}
