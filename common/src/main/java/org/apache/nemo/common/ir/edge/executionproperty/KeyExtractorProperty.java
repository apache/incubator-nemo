package org.apache.nemo.common.ir.edge.executionproperty;

import org.apache.nemo.common.KeyExtractor;
import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;

/**
 * KeyExtractor ExecutionProperty.
 */
public final class KeyExtractorProperty extends EdgeExecutionProperty<KeyExtractor> {
  /**
   * Constructor.
   *
   * @param value value of the execution property.
   */
  private KeyExtractorProperty(final KeyExtractor value) {
    super(value);
  }

  /**
   * Static method exposing the constructor.
   *
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static KeyExtractorProperty of(final KeyExtractor value) {
    return new KeyExtractorProperty(value);
  }
}
