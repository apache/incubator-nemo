package edu.snu.coral.common.exception;

/**
 * InvalidParameterException.
 * Thrown when the given parameters are invalid values in Runtime components.
 */
public final class InvalidParameterException extends RuntimeException {
  /**
   * InvalidParameterException.
   * @param message message
   */
  public InvalidParameterException(final String message) {
    super(message);
  }
}
