package org.apache.nemo.common.exception;

/**
 * UnsupportedMetricException.
 * This exception will be thrown when MetricStore receives unsupported metric.
 */
public final class UnsupportedMetricException extends RuntimeException {
  /**
   * UnsupportedMetricException.
   * @param cause cause
   */
  public UnsupportedMetricException(final Throwable cause) {
    super(cause);
  }
}
