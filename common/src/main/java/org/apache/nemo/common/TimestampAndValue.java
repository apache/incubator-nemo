package org.apache.nemo.common;

public final class TimestampAndValue<T> {
  public final long timestamp;
  public final T value;

  public TimestampAndValue(final long timestamp,
                           final T value) {
    this.timestamp = timestamp;
    this.value = value;
  }

  @Override
  public String toString() {
    return "timestamp: " + timestamp + ", value: " + value.toString();
  }
}
