package org.apache.nemo.common.exception;

/**
 * CompileTimeOptimizationException.
 * Thrown for compile-time optimization related exceptions.
 */
public class CompileTimeOptimizationException extends RuntimeException {
  /**
   * Constructor of CompileTimeOptimizationException.
   *
   * @param cause cause.
   */
  public CompileTimeOptimizationException(final Throwable cause) {
    super(cause);
  }

  /**
   * Constructor of CompileTimeOptimizationException.
   *
   * @param message message.
   */
  public CompileTimeOptimizationException(final String message) {
    super(message);
  }
}
