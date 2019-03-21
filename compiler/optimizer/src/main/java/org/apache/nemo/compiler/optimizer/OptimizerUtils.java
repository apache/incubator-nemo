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

package org.apache.nemo.compiler.optimizer;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.Util;
import org.apache.nemo.common.exception.InvalidParameterException;
import org.apache.nemo.common.exception.MetricException;
import org.apache.nemo.common.exception.UnsupportedMethodException;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Utility class for optimizer.
 */
public final class OptimizerUtils {
  private static final BlockingQueue<String> MESSAGE_BUFFER = new LinkedBlockingQueue<>();

  /**
   * Private constructor.
   */
  private OptimizerUtils() {
  }

  /**
   * @param message push the message to the message buffer.
   */
  public static void pushMessageBuffer(final String message) {
    MESSAGE_BUFFER.add(message);
  }

  /**
   * @return the message buffer.
   */
  public static String takeFromMessageBuffer() {
    try {
      return MESSAGE_BUFFER.take();
    } catch (InterruptedException e) {
      throw new MetricException("Interrupted while waiting for message: " + e);
    }
  }

  /**
   * Restore the formatted string into a pair of vertex/edge id and the execution property.
   * @param string the formatted string.
   * @return a pair of vertex/edge id and the execution property key index.
   */
  public static Pair<String, Integer> stringToIdAndEPKeyIndex(
    final String string) {
    if (string.length() != 9) {
      throw new InvalidParameterException("The metric data should follow the format of "
        + "[0]: index indicating vertex/edge, [1-4]: id of the component, and [5-8]: EP Key index");
    }
    final Integer idx = Integer.parseInt(string.substring(0, 1));
    final Integer numericId = Integer.parseInt(string.substring(1, 5));
    final String id;
    if (idx == 1) {
      id = Util.restoreVertexId(numericId);
    } else if (idx == 2) {
      id = Util.restoreEdgeId(numericId);
    } else {
      throw new UnsupportedMethodException("The index " + idx + " cannot be categorized into a vertex or an edge");
    }
    return Pair.of(id, Integer.parseInt(string.substring(5, 9)));
  }

  /**
   * Method to infiltrate keyword-containing string into the enum of Types above.
   * @param environmentType the input string.
   * @return the formatted string corresponding to each type.
   */
  public static String filterEnvironmentTypeString(final String environmentType) {
    if (environmentType.toLowerCase().contains("transient")) {
      return "transient";
    } else if (environmentType.toLowerCase().contains("large") && environmentType.toLowerCase().contains("shuffle")) {
      return "large_shuffle";
    } else if (environmentType.toLowerCase().contains("disaggregat")) {
      return "disaggregation";
    } else if (environmentType.toLowerCase().contains("stream")) {
      return "streaming";
    } else if (environmentType.toLowerCase().contains("small")) {
      return "small_size";
    } else if (environmentType.toLowerCase().contains("skew")) {
      return "data_skew";
    } else {
      return "";  // Default
    }
  }
}
