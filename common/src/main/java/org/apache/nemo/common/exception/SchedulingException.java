package org.apache.nemo.common.exception;

/**
 * SchedulingException.
 * Thrown when any exception occurs while trying to schedule
 * a {org.apache.nemo.runtime.common.plan.physical.Task} to an executor.
 */
public final class SchedulingException extends RuntimeException {
  /**
   * SchedulingException.
   * @param exception exception
   */
  public SchedulingException(final Throwable exception) {
    super(exception);
  }
}
