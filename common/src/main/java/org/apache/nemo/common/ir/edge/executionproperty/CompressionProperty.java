package org.apache.nemo.common.ir.edge.executionproperty;

import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;

/**
 * Compression ExecutionProperty.
 */
public final class CompressionProperty extends EdgeExecutionProperty<CompressionProperty.Value> {
  /**
   * Constructor.
   *
   * @param value value of the execution property.
   */
  private CompressionProperty(final Value value) {
    super(value);
  }

  /**
   * Static method exposing the constructor.
   *
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static CompressionProperty of(final Value value) {
    return new CompressionProperty(value);
  }

  /**
   * Possible values of Compression ExecutionProperty.
   */
  public enum Value {
    Gzip,
    LZ4,
    None
  }
}
