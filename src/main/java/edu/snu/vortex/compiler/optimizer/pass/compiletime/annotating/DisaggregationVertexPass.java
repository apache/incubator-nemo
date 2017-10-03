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
package edu.snu.vortex.compiler.optimizer.pass.compiletime.annotating;

import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.vortex.compiler.ir.executionproperty.vertex.ExecutorPlacementProperty;

/**
 * A pass to support Disaggregated Resources by tagging vertices.
 * This pass handles the ExecutorPlacement ExecutionProperty.
 */
public final class DisaggregationVertexPass extends AnnotatingPass {
  public static final String SIMPLE_NAME = "DisaggregationVertexPass";

  public DisaggregationVertexPass() {
    super(ExecutionProperty.Key.ExecutorPlacement);
  }

  @Override
  public String getName() {
    return SIMPLE_NAME;
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.topologicalDo(vertex ->
        vertex.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.COMPUTE)));
    return dag;
  }
}
