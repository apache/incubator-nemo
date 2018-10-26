package org.apache.nemo.common.exception;

/**
 * BlockWriteException.
 * Thrown when any exception occurs while trying to write a block.
 */
public final class BlockWriteException extends RuntimeException {
  /**
   * BlockWriteException.
   *
   * @param throwable the throwable to throw.
   */
  public BlockWriteException(final Throwable throwable) {
    super(throwable);
  }
}
