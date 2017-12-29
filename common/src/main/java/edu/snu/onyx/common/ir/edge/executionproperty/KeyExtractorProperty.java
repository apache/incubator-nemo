package edu.snu.onyx.common.ir.edge.executionproperty;

import edu.snu.onyx.common.KeyExtractor;
import edu.snu.onyx.common.ir.executionproperty.ExecutionProperty;

/**
 * KeyExtractor ExecutionProperty.
 */
public final class KeyExtractorProperty extends ExecutionProperty<KeyExtractor> {
  /**
   * Constructor.
   * @param value value of the execution property.
   */
  private KeyExtractorProperty(final KeyExtractor value) {
    super(Key.KeyExtractor, value);
  }

  /**
   * Static method exposing the constructor.
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static KeyExtractorProperty of(final KeyExtractor value) {
    return new KeyExtractorProperty(value);
  }
}
