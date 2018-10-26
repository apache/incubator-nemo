package org.apache.nemo.common.exception;

/**
 * UnsupportedCommPatternException.
 * Thrown when the intermediate data communication pattern is not supported in Runtime.
 */
public final class UnsupportedCommPatternException extends RuntimeException {
  /**
   * UnsupportedCommPatternException.
   * @param cause cause
   */
  public UnsupportedCommPatternException(final Throwable cause) {
    super(cause);
  }
}
