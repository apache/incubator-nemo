package org.apache.nemo.runtime.master.scheduler;

import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Functions to test schedulability with a pair of an executor and a task.
 */
@DriverSide
@ThreadSafe
@FunctionalInterface
public interface SchedulingConstraint {
  boolean testSchedulability(final ExecutorRepresenter executor, final Task task);
}
