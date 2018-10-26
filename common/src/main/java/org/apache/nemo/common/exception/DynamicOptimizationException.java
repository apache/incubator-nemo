package org.apache.nemo.common.exception;

/**
 * DynamicOptimizationException.
 * Thrown for dynamic optimization related exceptions.
 */
public class DynamicOptimizationException extends RuntimeException {
  /**
   * Constructor of DynamicOptimizationException.
   *
   * @param cause cause.
   */
  public DynamicOptimizationException(final Throwable cause) {
    super(cause);
  }

  /**
   * Constructor of DynamicOptimizationException.
   *
   * @param message message.
   */
  public DynamicOptimizationException(final String message) {
    super(message);
  }
}
