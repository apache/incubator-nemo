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
package org.apache.nemo.compiler.optimizer.examples;

import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.test.EmptyComponents;
import org.apache.nemo.compiler.optimizer.policy.DisaggregationPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.nemo.common.dag.DAG.EMPTY_DAG_DIRECTORY;

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
   *
   * @param args arguments.
   * @throws Exception Exceptions on the way.
   */
  public static void main(final String[] args) {
    final IRVertex source = new EmptyComponents.EmptySourceVertex<>("Source");
    final IRVertex map = new OperatorVertex(new EmptyComponents.EmptyTransform("MapVertex"));
    final IRVertex reduce = new OperatorVertex(new EmptyComponents.EmptyTransform("ReduceVertex"));

    // Before
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
    builder.addVertex(source);
    builder.addVertex(map);
    builder.addVertex(reduce);

    final IREdge edge1 = new IREdge(CommunicationPatternProperty.Value.ONE_TO_ONE, source, map);
    builder.connectVertices(edge1);

    final IREdge edge2 = new IREdge(CommunicationPatternProperty.Value.SHUFFLE, map, reduce);
    builder.connectVertices(edge2);

    final IRDAG dag = new IRDAG(builder.build());
    LOG.info("Before Optimization");
    LOG.info(dag.toString());

    // Optimize
    final IRDAG optimizedDAG = new DisaggregationPolicy().runCompileTimeOptimization(dag, EMPTY_DAG_DIRECTORY);

    // After
    LOG.info("After Optimization");
    LOG.info(optimizedDAG.toString());
  }
}
