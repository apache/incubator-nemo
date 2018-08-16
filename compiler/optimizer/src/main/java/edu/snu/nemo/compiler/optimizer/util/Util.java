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
package edu.snu.nemo.compiler.optimizer.util;

import java.util.function.IntPredicate;

/**
 * Class to hold the utility methods for compiler optimizer.
 */
public final class Util {

    /**
     * Private constructor for utility class.
     */
    private Util() { }

    /**
     * Check the equality of two intPredicates.
     * Check if the both the predicates either passes together or fails together for each
     * integer in the range [0,noOfTimes]
     *
     * @param firstPredicate the first IntPredicate.
     * @param secondPredicate the second IntPredicate.
     * @param noOfTimes Number to check the IntPredicates from.
     * @return whether or not we can say that they are equal.
     */
    public static boolean checkEqualityOfIntPredicates(final IntPredicate firstPredicate,
                                                 final IntPredicate secondPredicate,
                                                 final int noOfTimes) {
      for (int value = 0; value <= noOfTimes; value++) {
          if (firstPredicate.test(value) != secondPredicate.test(value)) {
              return false;
          }
      }
      return true;
    }
}
