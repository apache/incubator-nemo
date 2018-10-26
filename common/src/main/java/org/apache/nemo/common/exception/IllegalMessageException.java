package org.apache.nemo.common.exception;

/**
 * IllegalMessageException.
 * Thrown when the received message is of an illegal type in master/executor.
 */
public final class IllegalMessageException extends RuntimeException {
  /**
   * IllegalMessageException.
   * @param cause cause
   */
  public IllegalMessageException(final Throwable cause) {
    super(cause);
  }
}
