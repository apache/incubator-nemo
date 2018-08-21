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
package edu.snu.nemo.compiler.optimizer;

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * An interface for optimizer, which manages the optimization over submitted IR DAGs through
 * {@link edu.snu.nemo.compiler.optimizer.policy.Policy}s.
 */
@DefaultImplementation(NemoOptimizer.class)
public interface Optimizer {

  /**
   * Optimize the submitted DAG.
   *
   * @param dag the input DAG to optimize.
   * @return optimized DAG, reshaped or tagged with execution properties.
   */
  DAG<IRVertex, IREdge> optimizeDag(DAG<IRVertex, IREdge> dag);
}
