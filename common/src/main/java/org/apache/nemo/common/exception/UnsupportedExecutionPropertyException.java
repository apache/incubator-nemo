package org.apache.nemo.common.exception;

/**
 * UnsupportedExecutionPropertyException.
 * Thrown when Runtime does not support the execution property or it is unknown.
 */
public final class UnsupportedExecutionPropertyException extends RuntimeException {
  /**
   * UnsupportedExecutionPropertyException.
   * @param message message
   */
  public UnsupportedExecutionPropertyException(final String message) {
    super(message);
  }
}
