/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.common;

import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.AdditionalOutputTagProperty;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DecoderProperty;
import org.apache.nemo.common.ir.edge.executionproperty.EncoderProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;

import java.util.function.IntPredicate;

/**
 * Class to hold the utility methods.
 */
public final class Util {

  /**
   * Private constructor for utility class.
   */
  private Util() {
  }

  /**
   * Check the equality of two intPredicates.
   * Check if the both the predicates either passes together or fails together for each
   * integer in the range [0,noOfTimes]
   *
   * @param firstPredicate  the first IntPredicate.
   * @param secondPredicate the second IntPredicate.
   * @param noOfTimes       Number to check the IntPredicates from.
   * @return whether or not we can say that they are equal.
   */
  public static boolean checkEqualityOfIntPredicates(final IntPredicate firstPredicate,
                                                     final IntPredicate secondPredicate, final int noOfTimes) {
    for (int value = 0; value <= noOfTimes; value++) {
      if (firstPredicate.test(value) != secondPredicate.test(value)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Creates a new edge with several execution properties same as the given edge.
   * The copied execution properties include those minimally required for execution, such as commPattern and encoding.
   *
   * @param edgeToClone to copy execution properties from.
   * @param newSrc of the new edge.
   * @param newDst of the new edge.
   * @return the new edge.
   */
  public static IREdge cloneEdge(final IREdge edgeToClone,
                                 final IRVertex newSrc,
                                 final IRVertex newDst) {
    return cloneEdge(
      edgeToClone.getPropertyValue(CommunicationPatternProperty.class).get(), edgeToClone, newSrc, newDst);
  }

  /**
   * @param commPattern to use.
   * @param edgeToClone to copy execution properties from.
   * @param newSrc of the new edge.
   * @param newDst of the new edge.
   * @return the new edge.
   */
  public static IREdge cloneEdge(final CommunicationPatternProperty.Value commPattern,
                                 final IREdge edgeToClone,
                                 final IRVertex newSrc,
                                 final IRVertex newDst) {
    final IREdge clone = new IREdge(commPattern, newSrc, newDst);
    clone.setProperty(EncoderProperty.of(edgeToClone.getPropertyValue(EncoderProperty.class).get()));
    clone.setProperty(DecoderProperty.of(edgeToClone.getPropertyValue(DecoderProperty.class).get()));
    edgeToClone.getPropertyValue(AdditionalOutputTagProperty.class).ifPresent(tag -> {
      clone.setProperty(AdditionalOutputTagProperty.of(tag));
    });
    return clone;
  }

  /**
   * @param src vertex.
   * @param dst vertex.
   * @return the control edge.
   */
  public static IREdge getControlEdge(final IRVertex src, final IRVertex dst) {
  }
}
