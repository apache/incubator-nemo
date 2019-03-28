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

import org.apache.nemo.common.exception.MetricException;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.utility.MessageAggregatorVertex;
import org.apache.nemo.common.ir.vertex.utility.MessageBarrierVertex;
import org.apache.nemo.common.ir.vertex.utility.SamplingVertex;
import org.apache.nemo.common.ir.vertex.utility.StreamVertex;

import java.io.IOException;
import java.lang.instrument.Instrumentation;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Optional;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Class to hold the utility methods.
 */
public final class Util {
  // Assume that this tag is never used in user application
  private static final String CONTROL_EDGE_TAG = "CONTROL_EDGE";

  private static Instrumentation instrumentation;

  /**
   * Private constructor for utility class.
   */
  private Util() {
  }

  /**
   * Finds the project root path.
   *
   * @return the project root path.
   */
  public static String fetchProjectRootPath() {
    final String nemoHome = System.getenv("NEMO_HOME");
    if (!nemoHome.isEmpty()) {
      return nemoHome;
    } else {
      return recursivelyFindLicense(Paths.get(System.getProperty("user.dir")));
    }
  }

  /**
   * Helper method to recursively find the LICENSE file.
   *
   * @param path the path to search for.
   * @return the path containing the LICENSE file.
   */
  static String recursivelyFindLicense(final Path path) {
    try (Stream stream = Files.find(path, 1, (p, attributes) -> p.endsWith("LICENSE"))) {
      if (stream.count() > 0) {
        return path.toAbsolutePath().toString();
      } else {
        return recursivelyFindLicense(path.getParent());
      }
    } catch (NullPointerException e) {
      return System.getProperty("user.dir");
    } catch (IOException e) {
      throw new MetricException(e);
    }
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
   * @param newSrc      of the new edge.
   * @param newDst      of the new edge.
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
   * @param newSrc      of the new edge.
   * @param newDst      of the new edge.
   * @return the new edge.
   */
  public static IREdge cloneEdge(final CommunicationPatternProperty.Value commPattern,
                                 final IREdge edgeToClone,
                                 final IRVertex newSrc,
                                 final IRVertex newDst) {
    final IREdge clone = new IREdge(commPattern, newSrc, newDst);
    clone.setProperty(EncoderProperty.of(edgeToClone.getPropertyValue(EncoderProperty.class)
      .orElseThrow(IllegalStateException::new)));
    clone.setProperty(DecoderProperty.of(edgeToClone.getPropertyValue(DecoderProperty.class)
      .orElseThrow(IllegalStateException::new)));

    edgeToClone.getPropertyValue(AdditionalOutputTagProperty.class).ifPresent(tag -> {
      clone.setProperty(AdditionalOutputTagProperty.of(tag));
    });

    if (commPattern.equals(CommunicationPatternProperty.Value.Shuffle)) {
      edgeToClone.getPropertyValue(PartitionerProperty.class).ifPresent(p -> {
        if (p.right() == PartitionerProperty.NUM_EQUAL_TO_DST_PARALLELISM) {
          clone.setProperty(PartitionerProperty.of(p.left()));
        } else {
          clone.setProperty(PartitionerProperty.of(p.left(), p.right()));
        }
      });
    }

    edgeToClone.getPropertyValue(KeyExtractorProperty.class).ifPresent(ke -> {
      clone.setProperty(KeyExtractorProperty.of(ke));
    });
    edgeToClone.getPropertyValue(KeyEncoderProperty.class).ifPresent(keyEncoder -> {
      clone.setProperty(KeyEncoderProperty.of(keyEncoder));
    });
    edgeToClone.getPropertyValue(KeyDecoderProperty.class).ifPresent(keyDecoder -> {
      clone.setProperty(KeyDecoderProperty.of(keyDecoder));
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

  public static boolean isControlEdge(final IREdge edge) {
    return edge.getPropertyValue(AdditionalOutputTagProperty.class).equals(Optional.of(Util.CONTROL_EDGE_TAG));
  }

  public static boolean isUtilityVertex(final IRVertex v) {
    return v instanceof SamplingVertex
      || v instanceof MessageAggregatorVertex
      || v instanceof MessageBarrierVertex
      || v instanceof StreamVertex;
  }

  /**
   * @param vertices to stringify ids of.
   * @return the string of ids.
   */
  public static String stringifyIRVertexIds(final Collection<IRVertex> vertices) {
    return vertices.stream().map(IRVertex::getId).sorted().collect(Collectors.toList()).toString();
  }

  /**
   * @param edges to stringify ids of.
   * @return the string of ids.
   */
  public static String stringifyIREdgeIds(final Collection<IREdge> edges) {
    return edges.stream().map(IREdge::getId).sorted().collect(Collectors.toList()).toString();
  }

  /**
   * Method to restore String ID from the numeric ID.
   *
   * @param numericId the numeric id.
   * @return the restored string ID.
   */
  public static String restoreVertexId(final Integer numericId) {
    return "vertex" + numericId;
  }

  /**
   * Method to restore String ID from the numeric ID.
   *
   * @param numericId the numeric id.
   * @return the restored string ID.
   */
  public static String restoreEdgeId(final Integer numericId) {
    return "edge" + numericId;
  }

  /**
   * Method for the instrumentation: for getting the object size.
   *
   * @param args arguments.
   * @param inst the instrumentation.
   */
  public static void premain(final String args, final Instrumentation inst) {
    instrumentation = inst;
  }

  /**
   * Get the object byte size.
   *
   * @param o object to measure.
   * @return the bytes of the object.
   */
  public static long getObjectSize(final Object o) {
    return instrumentation.getObjectSize(o);
  }
}
