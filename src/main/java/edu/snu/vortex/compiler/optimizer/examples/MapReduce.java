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
package edu.snu.vortex.compiler.optimizer.examples;

import edu.snu.vortex.compiler.ir.DAG;
import edu.snu.vortex.compiler.ir.DAGBuilder;
import edu.snu.vortex.compiler.ir.Edge;
import edu.snu.vortex.compiler.ir.operator.Do;
import edu.snu.vortex.compiler.ir.operator.Source;
import edu.snu.vortex.compiler.optimizer.Optimizer;
import edu.snu.vortex.utils.Pair;

import java.util.List;
import java.util.Map;

/**
 * A sample MapReduce application.
 */
public final class MapReduce {
  private MapReduce() {
  }

  public static void main(final String[] args) throws Exception {
    final EmptySource source = new EmptySource();
    final EmptyDo<String, Pair<String, Integer>, Void> map = new EmptyDo<>("MapOperator");
    final EmptyDo<Pair<String, Iterable<Integer>>, String, Void> reduce = new EmptyDo<>("ReduceOperator");

    // Before
    final DAGBuilder builder = new DAGBuilder();
    builder.addOperator(source);
    builder.addOperator(map);
    builder.addOperator(reduce);
    builder.connectOperators(source, map, Edge.Type.OneToOne);
    builder.connectOperators(map, reduce, Edge.Type.ScatterGather);
    final DAG dag = builder.build();
    System.out.println("Before Optimization");
    System.out.println(dag);

    // Optimize
    final Optimizer optimizer = new Optimizer();
    final DAG optimizedDAG = optimizer.optimize(dag, Optimizer.PolicyType.Disaggregation);

    // After
    System.out.println("After Optimization");
    System.out.println(optimizedDAG);
  }

  /**
   * An empty source operator.
   */
  private static class EmptySource extends Source {
    @Override
    public List<Reader> getReaders(final long desiredBundleSizeBytes) throws Exception {
      return null;
    }
  }

  /**
   * An empty Do operator.
   * @param <I> input type.
   * @param <O> ouput type.
   * @param <T>
   */
  private static class EmptyDo<I, O, T> extends Do<I, O, T> {
    private final String name;

    EmptyDo(final String name) {
      this.name = name;
    }

    @Override
    public final String toString() {
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
