package org.apache.nemo.common.exception;


/**
 * IllegalEdgeOperationException.
 * Thrown when an operation is conducted with a {org.apache.nemo.runtime.common.plan.stage.StageEdge}
 * that is unknown/invalid/out of scope.
 */
public final class IllegalEdgeOperationException extends RuntimeException {
  /**
   * IllegalEdgeOperationException.
   * @param cause cause
   */
  public IllegalEdgeOperationException(final Throwable cause) {
    super(cause);
  }
}
