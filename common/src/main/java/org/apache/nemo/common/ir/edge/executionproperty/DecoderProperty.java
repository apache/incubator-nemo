package org.apache.nemo.common.ir.edge.executionproperty;

import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;

/**
 * Decoder ExecutionProperty.
 */
public final class DecoderProperty extends EdgeExecutionProperty<DecoderFactory> {
  /**
   * Constructor.
   *
   * @param value value of the execution property.
   */
  private DecoderProperty(final DecoderFactory value) {
    super(value);
  }

  /**
   * Static method exposing the constructor.
   *
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static DecoderProperty of(final DecoderFactory value) {
    return new DecoderProperty(value);
  }
}
