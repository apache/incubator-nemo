/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.common;

import java.util.Arrays;

/**
 * Descriptor for hash range.
 */
public final class HashRange implements KeyRange<Integer> {
  private static final HashRange ALL = new HashRange(0, Integer.MAX_VALUE, false);
  private final int rangeBeginInclusive;
  private final int rangeEndExclusive;
  private boolean isSkewed;

  /**
   * Private constructor.
   * @param rangeBeginInclusive point at which the hash range starts (inclusive).
   * @param rangeEndExclusive point at which the hash range ends (exclusive).
   */
  private HashRange(final int rangeBeginInclusive, final int rangeEndExclusive, final boolean isSkewed) {
    if (rangeBeginInclusive < 0 || rangeEndExclusive < 0) {
      throw new RuntimeException("Each boundary value of the range have to be non-negative.");
    }
    this.rangeBeginInclusive = rangeBeginInclusive;
    this.rangeEndExclusive = rangeEndExclusive;
    this.isSkewed = isSkewed;
  }

  /**
   * @return Gets a hash range descriptor representing the whole data from a partition.
   */
  public static HashRange all() {
    return ALL;
  }

  /**
   * @param rangeStartInclusive the start of the range (inclusive)
   * @param rangeEndExclusive   the end of the range (exclusive)
   * @return A hash range descriptor representing [{@code rangeBeginInclusive}, {@code rangeEndExclusive})
   */
  public static HashRange of(final int rangeStartInclusive, final int rangeEndExclusive, final boolean isSkewed) {
    return new HashRange(rangeStartInclusive, rangeEndExclusive, isSkewed);
  }

  /**
   * @return whether this hash range descriptor represents the whole data or not.
   */
  @Override
  public boolean isAll() {
    return this.equals(ALL);
  }

  /**
   * @return the beginning of this range (inclusive).
   */
  @Override
  public Integer rangeBeginInclusive() {
    return rangeBeginInclusive;
  }

  /**
   * @return the end of the range (exclusive)
   */
  @Override
  public Integer rangeEndExclusive() {
    return rangeEndExclusive;
  }

  /**
   * @param i the value to test
   * @return {@code true} if this hash range includes the specified value, {@code false} otherwise
   */
  @Override
  public boolean includes(final Integer i) {
    return i >= rangeBeginInclusive && i < rangeEndExclusive;
  }

  /**
   * {@inheritDoc}
   * This method should be overridden for a readable representation of KeyRange.
   * The generic type K should override {@link Object}'s toString() as well.
   */
  @Override
  public String toString() {
    final StringBuilder printableKeyRange = new StringBuilder("[");
    printableKeyRange.append(rangeBeginInclusive()).append(", ").append(rangeEndExclusive()).append(")");

    return printableKeyRange.toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final HashRange hashRange = (HashRange) o;
    if (rangeBeginInclusive != hashRange.rangeBeginInclusive
        || rangeEndExclusive != hashRange.rangeEndExclusive
        || isSkewed != hashRange.isSkewed) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(new Object[] {
        rangeBeginInclusive,    //auto-boxed
        rangeEndExclusive, //auto-boxed
        isSkewed,
    });
  }

  public boolean isSkewed() {
    return isSkewed;
  }
}
