package edu.snu.onyx.common.ir.vertex.executionproperty;

import edu.snu.onyx.common.ir.executionproperty.ExecutionProperty;

/**
 * Compressor ExecutionProperty.
 */
public final class CompressionProperty extends ExecutionProperty<CompressionProperty.Compressor> {
  /**
   * Constructor.
   * @param value value of the execution property.
   */
  private CompressionProperty(final Compressor value) {
    super(Key.Compressor, value);
  }

  /**
   * Static method exposing the constructor.
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static CompressionProperty of(final Compressor value) {
    return new CompressionProperty(value);
  }

  /**
   * Possible values of Compressor ExecutionProperty.
   */
  public enum Compressor {
    Raw,
    Gzip,
    LZ4,
  }
}
