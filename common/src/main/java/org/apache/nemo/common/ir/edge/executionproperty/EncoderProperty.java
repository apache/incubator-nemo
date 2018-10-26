package org.apache.nemo.common.ir.edge.executionproperty;

import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;

/**
 * EncoderFactory ExecutionProperty.
 */
public final class EncoderProperty extends EdgeExecutionProperty<EncoderFactory> {
  /**
   * Constructor.
   *
   * @param value value of the execution property.
   */
  private EncoderProperty(final EncoderFactory value) {
    super(value);
  }

  /**
   * Static method exposing the constructor.
   *
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static EncoderProperty of(final EncoderFactory value) {
    return new EncoderProperty(value);
  }
}
