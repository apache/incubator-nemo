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

package edu.snu.nemo.compiler.optimizer.policy;

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.eventhandler.PubSubEventHandlerWrapper;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.composite.DefaultCompositePass;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.composite.LargeShuffleCompositePass;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.composite.LoopOptimizationCompositePass;
import org.apache.reef.tang.Injector;

/**
 * A policy to demonstrate the large shuffle optimization, witch batches disk seek during data shuffle, conditionally.
 */
public final class ConditionalLargeShufflePolicy implements Policy {
  public static final PolicyBuilder BUILDER =
      new PolicyBuilder(false)
          .registerCompileTimePass(new LargeShuffleCompositePass(), dag -> getMaxParallelism(dag) > 300)
          .registerCompileTimePass(new LoopOptimizationCompositePass())
          .registerCompileTimePass(new DefaultCompositePass());
  private final Policy policy;

  /**
   * Default constructor.
   */
  public ConditionalLargeShufflePolicy() {
    this.policy = BUILDER.build();
  }

  /**
   * Returns the maximum parallelism of the vertices of a IR DAG.
   * @param dag dag to observe.
   * @return the maximum parallelism, or 1 by default.
   */
  private static int getMaxParallelism(final DAG<IRVertex, IREdge> dag) {
    return dag.getVertices().stream()
        .mapToInt(vertex -> vertex.getPropertyValue(ParallelismProperty.class).orElse(1))
        .max().orElse(1);
  }

  @Override
  public DAG<IRVertex, IREdge> runCompileTimeOptimization(final DAG<IRVertex, IREdge> dag, final String dagDirectory)
      throws Exception {
    return this.policy.runCompileTimeOptimization(dag, dagDirectory);
  }

  @Override
  public void registerRunTimeOptimizations(final Injector injector, final PubSubEventHandlerWrapper pubSubWrapper) {
    this.policy.registerRunTimeOptimizations(injector, pubSubWrapper);
  }
}
