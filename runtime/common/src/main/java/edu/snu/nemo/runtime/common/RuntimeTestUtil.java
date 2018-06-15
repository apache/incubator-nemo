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
package edu.snu.nemo.runtime.common;

import edu.snu.nemo.common.Pair;

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
