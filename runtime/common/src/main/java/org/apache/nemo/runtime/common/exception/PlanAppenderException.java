package org.apache.nemo.runtime.common.exception;

/**
 * An exception which represents exception during appending plans.
 */
public final class PlanAppenderException extends RuntimeException {

  /**
   * Constructor with throwable.
   *
   * @param throwable the throwable to throw.
   */
  public PlanAppenderException(final Throwable throwable) {
    super(throwable);
  }

  /**
   * Constructor with String.
   *
   * @param message the exception message.
   */
  public PlanAppenderException(final String message) {
    super(message);
  }
}
