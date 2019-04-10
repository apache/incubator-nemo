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
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.runtime.common.metric.MetricUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility class for optimizer.
 */
public final class OptimizerUtils {

  /**
   * Private constructor.
   */
  private OptimizerUtils() {
  }

  /**
   * Restore the formatted string into a pair of vertex/edge list and the execution property.
   *
   * @param string the formatted string.
   * @param dag the IR DAG to observe.
   * @return a pair of vertex/edge list and the execution property key index.
   */
  public static Pair<List<Object>, Integer> stringToObjsAndEPKeyIndex(final String string, final IRDAG dag) {
    // Formatted into 9 digits: 0:vertex/edge 1-4:ID 5-8:EP Index.
    if (string.length() != 9) {
      throw new InvalidParameterException("The metric data should follow the format of "
        + "[0]: index indicating vertex/edge, [1-4]: id of the component, and [5-8]: EP Key index. Current: " + string);
    }
    final Integer idx = Integer.parseInt(string.substring(0, 1));
    final Integer numericId = Integer.parseInt(string.substring(1, 5));
    final List<Object> objectList = new ArrayList<>();
    if (idx == 1) {
      final String id = Util.restoreVertexId(numericId);
      objectList.add(dag.getVertexById(id));
    } else if (idx == 2) {
      final String id = Util.restoreEdgeId(numericId);
      objectList.add(dag.getEdgeById(id));
    } else {
      throw new UnsupportedMethodException("The index " + idx + " cannot be categorized into a vertex or an edge");
    }
    return Pair.of(objectList, Integer.parseInt(string.substring(5, 9)));
  }

  /**
   * Restore the formatted string into a pair of vertex/edge list and the execution property index.
   *
   * @param string the formatted string, for patterns.
   * @param dag the IR DAG to observe.
   * @return a pair of vertex/edge list and the execution property key index.
   */
  public static Pair<List<Object>, Integer> patternStringToObjsAndEPKeyIndex(final String string, final IRDAG dag) {
    // Formatted into 10 digits: 0:pattern(1) 1-3:pattern ID 4-5:vtx/edge index 6-9:EP Index.
    final Integer idx = Integer.parseInt(string.substring(0, 1));
    if (string.length() != 10 || idx != 1) {
      throw new InvalidParameterException("The metric data should follow the format of "
        + "[0]: index indicating vertex/edge, [1-3]: id of the pattern, [4-5]: vertex/edge, "
        + "and [6-9]: EP Key index. Current: " + string);
    }
    final IRDAG subDAG = MetricUtils.getSubDAGFromIndex(Integer.parseInt(string.substring(1, 4)));
    final IRVertex targetVertex = subDAG.getTopologicalSort().get(subDAG.getVertices().size() - 1);
    if (subDAG.getEdges().stream().anyMatch(e -> e.getSrc().equals(targetVertex))) {
      throw new MetricException(targetVertex.getId() + " is not the target vertex");
    }

    final Stream<IRVertex> targetVertexFromDAG = dag.getVertices().stream()
      .filter(v -> v.toString().equals(targetVertex.toString()))
      .filter(v -> subDAG.getVertices().size() == dag.getIncomingEdgesOf(v).size() + 1)
      .filter(v -> dag.getIncomingEdgesOf(v).stream()
        .map(e -> e.getSrc().toString())
        .allMatch(str1 -> subDAG.getIncomingEdgesOf(targetVertex).stream()
          .map(e -> e.getSrc().toString())
          .anyMatch(str2 -> str2.equals(str1))));
    final Integer index = Integer.parseInt(string.substring(4, 6));
    final List<Object> objectList = new ArrayList<>();
    if (index % 2 == 0) {  // vertex
      targetVertexFromDAG.forEach(objectList::add);
    } else {  // edge
      targetVertexFromDAG.forEach(v -> objectList.add(dag.getIncomingEdgesOf(v).stream()
        .sorted(Comparator.comparing(e -> e.getSrc().toString()))
        .collect(Collectors.toList()).get(index / 2)));
    }
    return Pair.of(objectList, Integer.parseInt(string.substring(6, 10)));
  }

  /**
   * Method to infiltrate keyword-containing string into the enum of Types above.
   *
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
