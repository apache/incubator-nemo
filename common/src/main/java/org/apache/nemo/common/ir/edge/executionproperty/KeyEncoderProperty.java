package org.apache.nemo.common.ir.edge.executionproperty;

import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;

/**
 * KeyEncoder ExecutionProperty.
 */
public final class KeyEncoderProperty extends EdgeExecutionProperty<EncoderFactory> {
  /**
   * Constructor.
   *
   * @param value value of the execution property.
   */
  private KeyEncoderProperty(final EncoderFactory value) {
    super(value);
  }

  /**
   * Static method exposing the constructor.
   *
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static KeyEncoderProperty of(final EncoderFactory value) {
    return new KeyEncoderProperty(value);
  }
}
