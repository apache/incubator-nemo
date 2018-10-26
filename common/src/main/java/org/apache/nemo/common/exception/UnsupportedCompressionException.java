package org.apache.nemo.common.exception;

/**
 * UnsupportedCompressionException
 * Thrown when the compression method is not supported.
 */
public final class UnsupportedCompressionException extends RuntimeException {
  /**
   * Constructor.
   *
   * @param cause cause
   */
  public UnsupportedCompressionException(final String cause) {
    super(cause);
  }
}
