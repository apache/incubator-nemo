package org.apache.nemo.runtime.common;

import org.apache.nemo.common.Pair;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Utility class for runtime unit tests.
 */
public final class RuntimeTestUtil {

  /**
   * Private constructor for utility class.
   */
  private RuntimeTestUtil() {
  }

  /**
   * Gets a list of integer pair elements in range.
   * @param start value of the range (inclusive).
   * @param end   value of the range (exclusive).
   * @return the list of elements.
   */
  public static List getRangedNumList(final int start,
                                               final int end) {
    final List numList = new ArrayList<>(end - start);
    IntStream.range(start, end).forEach(number -> numList.add(Pair.of(number, number)));
    return numList;
  }

  /**
   * Flattens a nested list of elements.
   *
   * @param listOfList to flattens.
   * @return the flattened list of elements.
   */
  public static List flatten(final List<List> listOfList) {
    return listOfList.stream().flatMap(list -> ((List<Object>) list).stream()).collect(Collectors.toList());
  }
}
