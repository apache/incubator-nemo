package org.apache.nemo.runtime.master.scheduler;

import org.apache.nemo.runtime.common.plan.Task;

import net.jcip.annotations.ThreadSafe;

import javax.inject.Inject;
import java.util.Collection;
import java.util.Optional;

/**
 * Points to a collection of pending tasks eligible for scheduling.
 * This pointer effectively points to a subset of a scheduling group.
 * Within the collection, the tasks can be scheduled in any order.
 */
@ThreadSafe
public final class PendingTaskCollectionPointer {
  private Collection<Task> curTaskCollection;

  @Inject
  private PendingTaskCollectionPointer() {
  }

  /**
   * This collection of tasks should take precedence over any previous collection of tasks.
   * @param newCollection to schedule.
   */
  synchronized void setToOverwrite(final Collection<Task> newCollection) {
    this.curTaskCollection = newCollection;
  }

  /**
   * This collection of tasks can be scheduled only if there's no collection of tasks to schedule at the moment.
   * @param newCollection to schedule
   */
  synchronized void setIfNull(final Collection<Task> newCollection) {
    if (this.curTaskCollection == null) {
      this.curTaskCollection = newCollection;
    }
  }

  /**
   * Take the whole collection of tasks to schedule, and set the pointer to null.
   * @return optional tasks to schedule
   */
  synchronized Optional<Collection<Task>> getAndSetNull() {
    final Collection<Task> cur = curTaskCollection;
    curTaskCollection = null;
    return Optional.ofNullable(cur);
  }
}
