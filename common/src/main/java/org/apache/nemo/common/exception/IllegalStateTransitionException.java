package org.apache.nemo.common.exception;

/**
 * IllegalStateTransitionException.
 * Thrown when the execution state transition is illegal.
 */
public final class IllegalStateTransitionException extends Exception {
  /**
   * IllegalStateTransitionException.
   * @param cause cause
   */
  public IllegalStateTransitionException(final Throwable cause) {
    super(cause);
  }
}
