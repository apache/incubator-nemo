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

import java.util.*;

public final class DAGBuilder {
  private Map<String, List<Edge>> id2inEdges;
  private Map<String, List<Edge>> id2outEdges;
  private List<Operator> operators;

  public DAGBuilder() {
    this.id2inEdges = new HashMap<>();
    this.id2outEdges = new HashMap<>();
    this.operators = new ArrayList<>();
  }

  public DAGBuilder(final DAG dag) {
    this.operators = dag.getOperators();
    this.id2inEdges = dag.getId2inEdges();
    this.id2outEdges = dag.getId2outEdges();
  }

  /**
   * add an operator.
   * @param operator
   */
  public void addOperator(final Operator operator) {
    if (this.contains(operator)) {
      throw new RuntimeException("DAGBuilder is trying to add an operator multiple times");
    }
    operators.add(operator);
  }

  /**
   * add an edge for the given operators.
   * @param src
   * @param dst
   * @param type
   * @return
   */
  public <I, O> Edge<I, O> connectOperators(final Operator<?, I> src, final Operator<O, ?> dst, final Edge.Type type) {
    final Edge<I, O> edge = new Edge<>(type, src, dst);
    if (this.contains(edge)) {
      throw new RuntimeException("DAGBuilder is trying to add an edge multiple times");
    }
    addToEdgeList(id2outEdges, src.getId(), edge);
    addToEdgeList(id2inEdges, dst.getId(), edge);
    return edge;
  }

  public <I, O> Edge<I, O> connectOperators(final Edge edge) {
    if (this.contains(edge)) {
      throw new RuntimeException("DAGBuilder is trying to add an edge multiple times");
    }
    addToEdgeList(id2outEdges, edge.getSrc().getId(), edge);
    addToEdgeList(id2inEdges, edge.getDst().getId(), edge);
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

  public List<Operator> getOperators() {
    return operators;
  }

  /**
   * check if the DAGBuilder contains the operator
   * @param operator
   * @return
   */
  public boolean contains(Operator operator) {
    return operators.contains(operator);
  }

  /**
   * check if the DAGBuilder contains the edge
   * @param edge
   * @return
   */
  public boolean contains(Edge edge) {
    return (id2inEdges.containsValue(edge) || id2outEdges.containsValue(edge));
  }

  /**
   * returns the number of operators in the DAGBuilder
   * @return
   */
  public int size() {
    return operators.size();
  }

  /**
   * build the DAG
   * @return
   */
  public DAG build() {
    // TODO #22: DAG Integrity Check
    final boolean sourceCheck = operators.stream()
        .filter(operator -> !id2inEdges.containsKey(operator.getId()))
        .allMatch(operator -> operator instanceof Source);

    if (!sourceCheck) {
      throw new RuntimeException("DAG integrity unsatisfied: there are root operators that are not Sources.");
    }

    return new DAG(operators, id2inEdges, id2outEdges);
  }
}
