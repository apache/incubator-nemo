package org.apache.nemo.common.exception;

/**
 * IllegalVertexOperationException.
 * Thrown when an operation is conducted with a {org.apache.nemo.common.ir.IRVertex}
 * that is unknown/invalid/out of scope.
 */
public final class IllegalVertexOperationException extends RuntimeException {
  /**
   * IllegalVertexOperationException.
   * @param message message
   */
  public IllegalVertexOperationException(final String message) {
    super(message);
  }
}
