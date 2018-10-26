package org.apache.nemo.common.exception;

/**
 * UnsupportedBlockStoreException.
 * Thrown when the data placement method is undefined in Runtime.
 */
public final class UnsupportedBlockStoreException extends RuntimeException {
  /**
   * UnsupportedBlockStoreException.
   * @param cause cause
   */
  public UnsupportedBlockStoreException(final Throwable cause) {
    super(cause);
  }
}
