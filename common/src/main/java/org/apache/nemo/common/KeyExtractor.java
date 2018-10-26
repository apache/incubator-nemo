package org.apache.nemo.common;

import java.io.Serializable;

/**
 * Extracts a key from an element.
 * Keys are used for partitioning.
 */
public interface KeyExtractor extends Serializable {
  /**
   * Extracts key.
   *
   * @param element Element to get the key from.
   * @return The extracted key of the element.
   */
  Object extractKey(final Object element);
}
