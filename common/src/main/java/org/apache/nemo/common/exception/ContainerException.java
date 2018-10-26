package org.apache.nemo.common.exception;

/**
 * ContainerException.
 * Thrown for container/resource related exceptions.
 */
public final class ContainerException extends RuntimeException {
  /**
   * ContainerException.
   * @param cause cause
   */
  public ContainerException(final Throwable cause) {
    super(cause);
  }
}
