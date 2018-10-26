package org.apache.nemo.common.exception;

/**
 * UnknownExecutionStateException.
 * Thrown when the execution state is undefined in Runtime.
 */
public final class UnknownExecutionStateException extends RuntimeException {
  /**
   * UnknownExecutionStateException.
   * @param cause cause
   */
  public UnknownExecutionStateException(final Throwable cause) {
    super(cause);
  }
}
