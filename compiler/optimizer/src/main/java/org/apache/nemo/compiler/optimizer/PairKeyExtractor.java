package org.apache.nemo.compiler.optimizer;

import org.apache.nemo.common.KeyExtractor;
import org.apache.nemo.common.Pair;

/**
 * Extracts the key from a pair element.
 */
public final class PairKeyExtractor implements KeyExtractor {
  @Override
  public Object extractKey(final Object element) {
    if (element instanceof Pair) {
      return ((Pair) element).left();
    } else {
      throw new IllegalStateException(element.toString());
    }
  }
}
