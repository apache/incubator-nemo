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
package edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating;

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.executionproperty.ClonedSchedulingProperty;

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
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    // Speculative execution policy.
    final double fractionToWaitFor = 0.00000001; // Aggressive
    final double medianTimeMultiplier = 1.00000001; // Aggressive

    // Apply the policy to ALL vertices
    dag.getVertices().forEach(vertex -> vertex.setProperty(ClonedSchedulingProperty.of(
      new ClonedSchedulingProperty.CloneConf(fractionToWaitFor, medianTimeMultiplier))));
    return dag;
  }
}
