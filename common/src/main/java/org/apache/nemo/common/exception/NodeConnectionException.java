package org.apache.nemo.common.exception;

/**
 * NodeConnectionException.
 * Thrown when an exception occurs while trying to connect to a node.
 */
public final class NodeConnectionException extends RuntimeException {
  /**
   * NodeConnectionException.
   * @param cause cause
   */
  public NodeConnectionException(final Throwable cause) {
    super(cause);
  }
}
