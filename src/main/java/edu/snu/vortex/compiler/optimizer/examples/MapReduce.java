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
import edu.snu.vortex.utils.dag.DAG;
import edu.snu.vortex.utils.dag.DAGBuilder;

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
    final IRVertex source = new OperatorVertex(new EmptyTransform("SourceVertex"));
    final IRVertex map = new OperatorVertex(new EmptyTransform("MapVertex"));
    final IRVertex reduce = new OperatorVertex(new EmptyTransform("ReduceVertex"));

    // Before
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
    builder.addVertex(source);
    builder.addVertex(map);
    builder.addVertex(reduce);

    final IREdge edge1 = new IREdge(IREdge.Type.OneToOne, source, map);
    builder.connectVertices(edge1);

    final IREdge edge2 = new IREdge(IREdge.Type.ScatterGather, map, reduce);
    builder.connectVertices(edge2);

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
