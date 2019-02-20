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
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.vertex.IRVertex;

import java.util.Collection;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;

/**
 * Class to hold the utility methods.
 */
public final class Util {
  // Assume that this tag is never used in user application
  public static final String CONTROL_EDGE_TAG = "CONTROL_EDGE";

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
   * Creates a new edge with several execution properties same as the given edge.
   * The copied execution properties include those minimally required for execution, such as encoder/decoders.
   *
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

    if (edgeToClone.getPropertySnapshot().containsKey(EncoderProperty.class)) {
      clone.setProperty(edgeToClone.getPropertySnapshot().get(EncoderProperty.class));
    } else {
      clone.setProperty(EncoderProperty.of(edgeToClone.getPropertyValue(EncoderProperty.class)
        .orElseThrow(IllegalStateException::new)));
    }

    if (edgeToClone.getPropertySnapshot().containsKey(DecoderProperty.class)) {
      clone.setProperty(edgeToClone.getPropertySnapshot().get(DecoderProperty.class));
    } else {
      clone.setProperty(DecoderProperty.of(edgeToClone.getPropertyValue(DecoderProperty.class)
        .orElseThrow(IllegalStateException::new)));
    }

    edgeToClone.getPropertyValue(AdditionalOutputTagProperty.class).ifPresent(tag -> {
      clone.setProperty(AdditionalOutputTagProperty.of(tag));
    });

    edgeToClone.getPropertyValue(PartitionerProperty.class).ifPresent(p -> {
      if (p.right() == PartitionerProperty.NUM_EQUAL_TO_DST_PARALLELISM) {
        clone.setProperty(PartitionerProperty.of(p.left()));
      } else {
        clone.setProperty(PartitionerProperty.of(p.left(), p.right()));
      }
    });

    edgeToClone.getPropertyValue(KeyExtractorProperty.class).ifPresent(ke -> {
      clone.setProperty(KeyExtractorProperty.of(ke));
    });

    return clone;
  }

  /**
   * A control edge enforces an execution ordering between the source vertex and the destination vertex.
   * The additional output tag property of control edges is set such that no actual data element is transferred
   * via the edges. This minimizes the run-time overhead of executing control edges.
   *
   * @param src vertex.
   * @param dst vertex.
   * @return the control edge.
   */
  public static IREdge createControlEdge(final IRVertex src, final IRVertex dst) {
    final IREdge controlEdge = new IREdge(CommunicationPatternProperty.Value.BroadCast, src, dst);
    controlEdge.setPropertyPermanently(AdditionalOutputTagProperty.of(CONTROL_EDGE_TAG));
    return controlEdge;
  }

  /**
   * @param vertices to stringify ids of.
   * @return the string of ids.
   */
  public static String stringifyIRVertexIds(final Collection<IRVertex> vertices) {
    return vertices.stream().map(IRVertex::getId).collect(Collectors.toSet()).toString();
  }

  /**
   * @param edges to stringify ids of.
   * @return the string of ids.
   */
  public static String stringifyIREdgeIds(final Collection<IREdge> edges) {
    return edges.stream().map(IREdge::getId).collect(Collectors.toSet()).toString();
  }
}
