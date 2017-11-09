package edu.snu.onyx.compiler.ir.executionproperty.edge;

import edu.snu.onyx.compiler.ir.KeyExtractor;
import edu.snu.onyx.compiler.ir.executionproperty.ExecutionProperty;

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
