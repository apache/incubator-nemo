package org.apache.nemo.runtime.executor.common;

import org.apache.reef.wake.Identifier;

/**
 */
public final class ByteTransportIdentifier implements Identifier {

  private final String executorId;

  /**
   * Creates a {@link ByteTransportIdentifier}.
   */
  public ByteTransportIdentifier(final String executorId) {
    this.executorId = executorId;
  }

  @Override
  public String toString() {
    return "byte://" + executorId;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ByteTransportIdentifier that = (ByteTransportIdentifier) o;
    return executorId.equals(that.executorId);
  }

  @Override
  public int hashCode() {
    return executorId.hashCode();
  }
}
