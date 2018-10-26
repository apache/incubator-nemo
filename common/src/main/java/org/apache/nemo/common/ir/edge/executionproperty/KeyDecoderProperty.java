package org.apache.nemo.common.ir.edge.executionproperty;

import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;

/**
 * KeyDecoder ExecutionProperty.
 */
public final class KeyDecoderProperty extends EdgeExecutionProperty<DecoderFactory> {
  /**
   * Constructor.
   *
   * @param value value of the execution property.
   */
  private KeyDecoderProperty(final DecoderFactory value) {
    super(value);
  }

  /**
   * Static method exposing the constructor.
   *
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static KeyDecoderProperty of(final DecoderFactory value) {
    return new KeyDecoderProperty(value);
  }
}
