package org.apache.nemo.common.exception;

/**
 * BlockFetchException.
 * Thrown when any exception occurs while trying to fetch a block for task execution.
 */
public final class BlockFetchException extends RuntimeException {
  /**
   * BlockFetchException.
   *
   * @param throwable the throwable to throw.
   */
  public BlockFetchException(final Throwable throwable) {
    super(throwable);
  }
}
