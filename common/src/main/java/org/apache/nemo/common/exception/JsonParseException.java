package org.apache.nemo.common.exception;

/**
 * JsonParseException.
 * Thrown when the cause for the json parsing failure.
 */
public final class JsonParseException extends RuntimeException {
  /**
   * JsonParseException.
   * @param cause cause
   */
  public JsonParseException(final Throwable cause) {
    super(cause);
  }
}
