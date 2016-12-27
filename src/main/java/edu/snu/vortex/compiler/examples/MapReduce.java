/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.vortex.compiler.examples;

import edu.snu.vortex.compiler.plan.Attributes;
import edu.snu.vortex.compiler.plan.DAG;
import edu.snu.vortex.compiler.plan.DAGBuilder;
import edu.snu.vortex.compiler.plan.Edge;
import edu.snu.vortex.compiler.plan.node.Do;
import edu.snu.vortex.compiler.plan.node.Node;
import edu.snu.vortex.compiler.plan.node.Source;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MapReduce {
  public static void main(final String[] args) {
    final EmptySource source = new EmptySource();
    final EmptyDo<String, Pair<String, Integer>, Void> map = new EmptyDo<>("MapOperator");
    final EmptyDo<Pair<String, Iterable<Integer>>, String, Void> reduce = new EmptyDo<>("ReduceOperator");

    // Before
    final DAGBuilder builder = new DAGBuilder();
    builder.addNode(source);
    builder.addNode(map);
    builder.addNode(reduce);
    builder.connectNodes(source, map, Edge.Type.O2O);
    builder.connectNodes(map, reduce, Edge.Type.M2M);
    final DAG dag = builder.build();
    System.out.println("Before Optimization");
    DAG.print(dag);

    // Optimize
    final List<Node> topoSorted = new LinkedList<>();
    DAG.doDFS(dag, (node -> topoSorted.add(0, node)), DAG.VisitOrder.PostOrder);
    topoSorted.forEach(node -> {
      final Optional<List<Edge>> inEdges = dag.getInEdges(node);
      if (!inEdges.isPresent()) {
        node.setAttr(Attributes.Key.Placement, Attributes.Placement.Compute);
      } else {
        node.setAttr(Attributes.Key.Placement, Attributes.Placement.Storage);
      }
    });

    // After
    System.out.println("After Optimization");
    DAG.print(dag);
  }

  private static class Pair<K, V> {
    private K key;
    private V val;

    Pair(final K key, final V val) {
      this.key = key;
      this.val = val;
    }
  }

  private static class EmptySource extends Source {
    @Override
    public List<Reader> getReaders(long desiredBundleSizeBytes) throws Exception {
      return null;
    }
  }

  private static class EmptyDo<I, O, T> extends Do<I, O, T> {
    private final String name;

    EmptyDo(final String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append(super.toString());
      sb.append(", name: ");
      sb.append(name);
      return sb.toString();
    }

    @Override
    public Iterable<O> transform(final Iterable<I> input, final Map<T, Object> broadcasted) {
      return null;
    }
  }

}
