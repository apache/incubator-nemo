package org.apache.nemo.compiler.frontend.beam;

import org.apache.beam.sdk.util.WindowedValue;
import org.apache.nemo.common.KeyExtractor;
import org.apache.beam.sdk.values.KV;

/**
 * Extracts the key from a KV element.
 * For non-KV elements, the elements themselves become the key.
 */
final class BeamKeyExtractor implements KeyExtractor {
  @Override
  public Object extractKey(final Object element) {
    final WindowedValue windowedValue = (WindowedValue) element;
    final Object value = windowedValue.getValue();
    if (value instanceof KV) {
      // Handle null keys, since Beam allows KV with null keys.
      final Object key = ((KV) value).getKey();
      return key == null ? 0 : key;
    } else {
      return element;
    }
  }
}
