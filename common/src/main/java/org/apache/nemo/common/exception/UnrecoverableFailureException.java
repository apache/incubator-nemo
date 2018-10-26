package org.apache.nemo.common.exception;

/**
 * UnrecoverableFailureException.
 * Thrown when a job fails and is unrecoverable.
 */
public final class UnrecoverableFailureException extends RuntimeException {
  /**
   * UnrecoverableFailureException.
   * @param cause cause
   */
  public UnrecoverableFailureException(final Throwable cause) {
    super(cause);
  }
}
