package edu.snu.onyx.common.ir.edge.executionproperty;

import edu.snu.onyx.common.KeyExtractor;
import edu.snu.onyx.common.ir.executionproperty.ExecutionProperty;

/**
 * KeyExtractor ExecutionProperty.
 */
public final class KeyExtractorProperty extends ExecutionProperty<KeyExtractor> {
  private KeyExtractorProperty(final KeyExtractor value) {
    super(Key.KeyExtractor, value);
  }

  public static KeyExtractorProperty of(final KeyExtractor value) {
    return new KeyExtractorProperty(value);
  }
}
