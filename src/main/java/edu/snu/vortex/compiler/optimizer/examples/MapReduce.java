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

import edu.snu.vortex.compiler.ir.*;
import edu.snu.vortex.compiler.optimizer.Optimizer;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A sample MapReduce application.
 */
public final class MapReduce {
  private static final Logger LOG = Logger.getLogger(MapReduce.class.getName());

  private MapReduce() {
  }

  public static void main(final String[] args) throws Exception {
    final Vertex source = new OperatorVertex(new EmptyTransform("SourceVertex"));
    final Vertex map = new OperatorVertex(new EmptyTransform("MapVertex"));
    final Vertex reduce = new OperatorVertex(new EmptyTransform("ReduceVertex"));

    // Before
    final DAGBuilder builder = new DAGBuilder();
    builder.addVertex(source);
    builder.addVertex(map);
    builder.addVertex(reduce);
    builder.connectVertices(source, map, Edge.Type.OneToOne);
    builder.connectVertices(map, reduce, Edge.Type.ScatterGather);
    final DAG dag = builder.build();
    LOG.log(Level.INFO, "Before Optimization");
    LOG.log(Level.INFO, dag.toString());

    // Optimize
    final Optimizer optimizer = new Optimizer();
    final DAG optimizedDAG = optimizer.optimize(dag, Optimizer.PolicyType.Disaggregation);

    // After
    LOG.log(Level.INFO, "After Optimization");
    LOG.log(Level.INFO, optimizedDAG.toString());
  }

  /**
   * An empty transform.
   */
  private static class EmptyTransform implements Transform {
    private final String name;

    EmptyTransform(final String name) {
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
    public void prepare(final Context context, final OutputCollector outputCollector) {
    }

    @Override
    public void onData(final Iterable<Element> data, final String srcVertexId) {
    }

    @Override
    public void close() {
    }
  }
}
