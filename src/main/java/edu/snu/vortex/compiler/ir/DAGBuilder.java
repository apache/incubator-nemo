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
package edu.snu.vortex.compiler.ir;

import edu.snu.vortex.compiler.ir.operator.Operator;
import edu.snu.vortex.compiler.ir.operator.Source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DAGBuilder {
  private Map<String, List<Edge>> id2inEdges;
  private Map<String, List<Edge>> id2outEdges;
  private List<Operator> operators;

  public DAGBuilder() {
    this.id2inEdges = new HashMap<>();
    this.id2outEdges = new HashMap<>();
    this.operators = new ArrayList<>();
  }

  public void addOperator(final Operator operator) {
    operators.add(operator);
  }

  public <I, O> Edge<I, O> connectOperators(final Operator<?, I> src, final Operator<O, ?> dst, final Edge.Type type) {
    final Edge<I, O> edge = new Edge<>(type, src, dst);
    addToEdgeList(id2inEdges, dst.getId(), edge);
    addToEdgeList(id2outEdges, src.getId(), edge);
    return edge;
  }

  private void addToEdgeList(final Map<String, List<Edge>> map, final String id, final Edge edge) {
    if (map.containsKey(id)) {
      map.get(id).add(edge);
    } else {
      final List<Edge> inEdges = new ArrayList<>(1);
      inEdges.add(edge);
      map.put(id, inEdges);
    }
  }

  public DAG build() {
    // TODO #22: DAG Integrity Check
    final List<Source> sources = operators.stream()
        .filter(operator -> !id2inEdges.containsKey(operator.getId()))
        .map(operator -> (Source)operator)
        .collect(Collectors.toList());
    return new DAG(sources, id2inEdges, id2outEdges);
  }
}
