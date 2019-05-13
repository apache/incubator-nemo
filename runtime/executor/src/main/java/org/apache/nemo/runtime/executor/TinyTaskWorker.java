package org.apache.nemo.runtime.executor;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.nemo.offloading.common.OffloadingWorker;
import org.apache.nemo.runtime.executor.common.OffloadingExecutorEventType;
import org.apache.nemo.runtime.lambdaexecutor.downstream.TaskEndEvent;
import org.apache.nemo.runtime.lambdaexecutor.general.OffloadingTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public final class TinyTaskWorker {
  private static final Logger LOG = LoggerFactory.getLogger(TinyTaskWorker.class.getName());

  private static final int SLOT = 5;

  private final List<OffloadingTask> offloadedTasks = new LinkedList<>();
  private final List<OffloadingTask> pendingTasks = new LinkedList<>();
  private final OffloadingWorker offloadingWorker;
  private final Coder<UnboundedSource.CheckpointMark> coder;
  private final AtomicInteger deletePending = new AtomicInteger(0);

  public TinyTaskWorker(final OffloadingWorker offloadingWorker,
                        final Coder<UnboundedSource.CheckpointMark> coder) {
    this.offloadingWorker = offloadingWorker;
    this.coder = coder;
  }

  public synchronized void addTask(final OffloadingTask task) {
    pendingTasks.add(task);
  }

  public boolean isReady() {
    return offloadingWorker.isReady();
  }

  public synchronized boolean canHandleTask() {
    if (offloadedTasks.size() + pendingTasks.size() >= SLOT) {
      return false;
    } else {
      return true;
    }
  }

  public synchronized boolean hasTask(final String taskid) {
    for (final OffloadingTask task : offloadedTasks) {
      if (task.taskId.equals(taskid)) {
        return true;
      }
    }

    for (final OffloadingTask task : pendingTasks) {
      if (task.taskId.equals(taskid)) {
        return true;
      }
    }

    return false;
  }

  private int findTask(final List<OffloadingTask> tasks, final String taskId) {
    int index = 0;
    for (final OffloadingTask task : tasks) {
      if (task.taskId.equals(taskId)) {
        return index;
      }
      index += 1;
    }

    return -1;
  }

  public synchronized int getNumScheduledTasks() {
    return offloadedTasks.size();
  }

  public synchronized int getNumPendingTasks() {
    return pendingTasks.size();
  }

  public synchronized boolean hasNoTask() {
    return pendingTasks.size() + offloadedTasks.size() == 0;
  }

  public synchronized boolean deleteTask(final String taskId) {
    int index = findTask(offloadedTasks, taskId);
    if (index >= 0) {
      // SEND end message of the task!
      final TaskEndEvent endEvent = new TaskEndEvent(taskId);
      final ByteBuf byteBuf = endEvent.encode();
      offloadingWorker.execute(byteBuf, 1, false);
      return false;
    } else {
      index = findTask(pendingTasks, taskId);
      if (index < 0) {
        throw new RuntimeException("no such task " + taskId + " in the tiny worker");
      }

      // just remove, because it is pending
      pendingTasks.remove(index);
      return true;
    }
  }

  public AtomicInteger getDeletePending() {
    return deletePending;
  }

  public synchronized void executePending() {

    if (pendingTasks.isEmpty()) {
      return;
    }

    LOG.info("Execute pending222 !!");

    for (final OffloadingTask pending : pendingTasks) {
      offloadingWorker.execute(pending.encode(coder), 1, false);
    }

    LOG.info("Execute pending333 !!");

    offloadedTasks.addAll(pendingTasks);
    pendingTasks.clear();
  }

  public void close() {
    LOG.info("Closing worker..!!");
    offloadingWorker.forceClose();
  }
}
