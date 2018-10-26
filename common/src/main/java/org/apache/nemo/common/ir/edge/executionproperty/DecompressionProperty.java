package org.apache.nemo.common.ir.edge.executionproperty;

import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;

/**
 * Decompression ExecutionProperty.
 * It shares the value with {@link CompressionProperty}.
 */
public final class DecompressionProperty extends EdgeExecutionProperty<CompressionProperty.Value> {
  /**
   * Constructor.
   *
   * @param value value of the execution property.
   */
  private DecompressionProperty(final CompressionProperty.Value value) {
    super(value);
  }

  /**
   * Static method exposing the constructor.
   *
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static DecompressionProperty of(final CompressionProperty.Value value) {
    return new DecompressionProperty(value);
  }
}
