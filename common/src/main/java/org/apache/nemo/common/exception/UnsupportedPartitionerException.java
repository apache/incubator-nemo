package org.apache.nemo.common.exception;

/**
 * UnsupportedPartitionerException.
 * Thrown when the intermediate data partitioning method is not supported in Runtime.
 */
public final class UnsupportedPartitionerException extends RuntimeException {
  /**
   * UnsupportedPartitionerException.
   * @param cause cause
   */
  public UnsupportedPartitionerException(final Throwable cause) {
    super(cause);
  }
}
