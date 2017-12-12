/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.onyx.runtime.common.data;

import java.io.Serializable;

/**
 * Descriptor for hash range.
 * TODO #494: Refactor HashRange to be general.
 */
public final class HashRange implements Serializable {
  private static final HashRange ALL = new HashRange(0, Integer.MAX_VALUE);

  private final int rangeStartInclusive;
  private final int rangeEndExclusive;

  /**
   * Private constructor.
   * @param rangeStartInclusive point at which the hash range starts (inclusive).
   * @param rangeEndExclusive point at which the hash range ends (exclusive).
   */
  private HashRange(final int rangeStartInclusive, final int rangeEndExclusive) {
    if (rangeStartInclusive < 0 || rangeEndExclusive < 0) {
      throw new RuntimeException("Each boundary value of the range have to be non-negative.");
    }
    this.rangeStartInclusive = rangeStartInclusive;
    this.rangeEndExclusive = rangeEndExclusive;
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
   * @return A hash range descriptor representing [{@code rangeStartInclusive}, {@code rangeEndExclusive})
   */
  public static HashRange of(final int rangeStartInclusive, final int rangeEndExclusive) {
    return new HashRange(rangeStartInclusive, rangeEndExclusive);
  }

  /**
   * @return whether this hash range descriptor represents the whole data or not
   */
  public boolean isAll() {
    return this.equals(ALL);
  }

  /**
   * @return the start of the range (inclusive)
   */
  public int rangeStartInclusive() {
    return rangeStartInclusive;
  }

  /**
   * @return the end of the range (exclusive)
   */
  public int rangeEndExclusive() {
    return rangeEndExclusive;
  }

  /**
   * @return the length of this range
   */
  public int length() {
    return rangeEndExclusive - rangeStartInclusive;
  }

  /**
   * @param i the value to test
   * @return {@code true} if this hash range includes the specified value, {@code false} otherwise
   */
  public boolean includes(final int i) {
    return i >= rangeStartInclusive && i < rangeEndExclusive;
  }

  @Override
  public String toString() {
    return String.format("[%d, %d)", rangeStartInclusive, rangeEndExclusive);
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
    if (rangeStartInclusive != hashRange.rangeStartInclusive) {
      return false;
    }
    return rangeEndExclusive == hashRange.rangeEndExclusive;
  }

  @Override
  public int hashCode() {
    int result = rangeStartInclusive;
    result = 31 * result + rangeEndExclusive;
    return result;
  }
}
