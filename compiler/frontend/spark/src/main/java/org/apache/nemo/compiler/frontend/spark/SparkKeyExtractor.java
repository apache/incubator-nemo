package org.apache.nemo.compiler.frontend.spark;

import org.apache.nemo.common.KeyExtractor;
import scala.Tuple2;

/**
 * Extracts the key from a KV element.
 * For non-KV elements, the elements themselves become the key.
 */
public final class SparkKeyExtractor implements KeyExtractor {
  @Override
  public Object extractKey(final Object element) {
    if (element instanceof Tuple2) {
      return ((Tuple2) element)._1;
    } else {
      return element;
    }
  }
}
