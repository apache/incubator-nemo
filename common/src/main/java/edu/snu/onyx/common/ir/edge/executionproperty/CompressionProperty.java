package edu.snu.onyx.common.ir.edge.executionproperty;

import edu.snu.onyx.common.ir.executionproperty.ExecutionProperty;

/**
 * Compression ExecutionProperty.
 */
public final class CompressionProperty extends ExecutionProperty<CompressionProperty.Compression> {
  /**
   * Constructor.
   * @param value value of the execution property.
   */
  private CompressionProperty(final Compression value) {
    super(Key.Compression, value);
  }

  /**
   * Static method exposing the constructor.
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static CompressionProperty of(final Compression value) {
    return new CompressionProperty(value);
  }

  /**
   * Possible values of Compression ExecutionProperty.
   */
  public enum Compression {
    Gzip,
    LZ4,
  }
}
