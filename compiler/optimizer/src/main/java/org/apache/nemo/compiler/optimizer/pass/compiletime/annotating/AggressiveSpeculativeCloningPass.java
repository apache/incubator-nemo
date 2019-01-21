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
package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ClonedSchedulingProperty;

/**
 * Speculative execution. (very aggressive, for unit tests)
 * TODO #200: Maintain Test Passes and Policies Separately
 */
@Annotates(ClonedSchedulingProperty.class)
public final class AggressiveSpeculativeCloningPass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public AggressiveSpeculativeCloningPass() {
    super(AggressiveSpeculativeCloningPass.class);
  }

  @Override
  public void optimize(final DAG<IRVertex, IREdge> dag) {
    // Speculative execution policy.
    final double fractionToWaitFor = 0.00000001; // Aggressive
    final double medianTimeMultiplier = 1.00000001; // Aggressive

    // Apply the policy to ALL vertices
    dag.getVertices().forEach(vertex -> vertex.setProperty(ClonedSchedulingProperty.of(
      new ClonedSchedulingProperty.CloneConf(fractionToWaitFor, medianTimeMultiplier))));
  }
}
