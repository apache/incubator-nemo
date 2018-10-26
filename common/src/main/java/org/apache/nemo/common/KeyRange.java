package org.apache.nemo.common;

import java.io.Serializable;

/**
 * Represents the key range of data partitions within a block.
 * @param <K> the type of key to assign for each partition.
 */
public interface KeyRange<K extends Serializable> extends Serializable {

 /**
   * @return whether this instance represents the entire range or not.
   */
  boolean isAll();

  /**
   * @return the beginning of this range (inclusive).
   */
  K rangeBeginInclusive();

  /**
   * @return the end of this range (exclusive).
   */
  K rangeEndExclusive();

  /**
   * @param key the value to check
   * @return {@code true} if this key range includes the specified value, {@code false} otherwise
   */
  boolean includes(final K key);

  /**
   * {@inheritDoc}
   * This method should be overridden for a readable representation of KeyRange.
   * The generic type K should override {@link Object}'s toString() as well.
   */
  @Override
  String toString();

  /**
   * {@inheritDoc}
   * This method should be overridden for KeyRange comparisons.
   */
  @Override
  boolean equals(final Object o);

  /**
   * {@inheritDoc}
   * This method should be overridden for KeyRange comparisons.
   */
  @Override
  int hashCode();
}
