package org.apache.nemo.common.exception;

/**
 * UnknownFailureCauseException.
 * Thrown when the cause for the recoverable failure is undefined in Runtime.
 */
public final class UnknownFailureCauseException extends RuntimeException {
  /**
   * UnknownFailureCauseException.
   * @param cause cause
   */
  public UnknownFailureCauseException(final Throwable cause) {
    super(cause);
  }
}
