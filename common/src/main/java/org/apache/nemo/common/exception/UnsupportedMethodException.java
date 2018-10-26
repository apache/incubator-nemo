package org.apache.nemo.common.exception;

/**
 * UnsupportedMethodException.
 * Thrown when a unsupported method in a class is called.
 */
public final class UnsupportedMethodException extends RuntimeException {
  /**
   * UnsupportedMethodException.
   * @param message message
   */
  public UnsupportedMethodException(final String message) {
    super(message);
  }
}
