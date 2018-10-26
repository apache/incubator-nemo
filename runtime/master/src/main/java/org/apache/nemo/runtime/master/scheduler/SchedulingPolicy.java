package org.apache.nemo.runtime.master.scheduler;

import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import net.jcip.annotations.ThreadSafe;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.Collection;

/**
 * A function to select an executor from collection of available executors.
 */
@DriverSide
@ThreadSafe
@FunctionalInterface
@DefaultImplementation(MinOccupancyFirstSchedulingPolicy.class)
public interface SchedulingPolicy {
  /**
   * A function to select an executor from the specified collection of available executors.
   *
   * @param executors The collection of available executors.
   *                  Implementations can assume that the collection is not empty.
   * @param task The task to schedule
   * @return The selected executor. It must be a member of {@code executors}.
   */
  ExecutorRepresenter selectExecutor(final Collection<ExecutorRepresenter> executors, final Task task);
}
