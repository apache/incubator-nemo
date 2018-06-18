/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.nemo.compiler.optimizer.examples;

import edu.snu.nemo.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.OperatorVertex;
import edu.snu.nemo.common.test.EmptyComponents;
import edu.snu.nemo.compiler.optimizer.policy.DisaggregationPolicy;
import edu.snu.nemo.compiler.optimizer.CompiletimeOptimizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static edu.snu.nemo.common.dag.DAG.EMPTY_DAG_DIRECTORY;

/**
 * A sample MapReduceDisaggregationOptimization application.
 */
public final class MapReduceDisaggregationOptimization {
  private static final Logger LOG = LoggerFactory.getLogger(MapReduceDisaggregationOptimization.class.getName());

  /**
   * Private constructor.
   */
  private MapReduceDisaggregationOptimization() {
  }

  /**
   * Main function of the example MR program.
   * @param args arguments.
   * @throws Exception Exceptions on the way.
   */
  public static void main(final String[] args) throws Exception {
    final IRVertex source = new EmptyComponents.EmptySourceVertex<>("Source");
    final IRVertex map = new OperatorVertex(new EmptyComponents.EmptyTransform("MapVertex"));
    final IRVertex reduce = new OperatorVertex(new EmptyComponents.EmptyTransform("ReduceVertex"));

    // Before
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
    builder.addVertex(source);
    builder.addVertex(map);
    builder.addVertex(reduce);

    final IREdge edge1 = new IREdge(DataCommunicationPatternProperty.Value.OneToOne, source, map);
    builder.connectVertices(edge1);

    final IREdge edge2 = new IREdge(DataCommunicationPatternProperty.Value.Shuffle, map, reduce);
    builder.connectVertices(edge2);

    final DAG<IRVertex, IREdge> dag = builder.build();
    LOG.info("Before Optimization");
    LOG.info(dag.toString());

    // Optimize
    final DAG optimizedDAG = CompiletimeOptimizer.optimize(dag, new DisaggregationPolicy(), EMPTY_DAG_DIRECTORY);

    // After
    LOG.info("After Optimization");
    LOG.info(optimizedDAG.toString());
  }
}
