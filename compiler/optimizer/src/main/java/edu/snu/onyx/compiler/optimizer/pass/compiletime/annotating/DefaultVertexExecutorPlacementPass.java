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
package edu.snu.onyx.compiler.optimizer.pass.compiletime.annotating;

import edu.snu.onyx.common.dag.DAG;
import edu.snu.onyx.common.ir.edge.IREdge;
import edu.snu.onyx.common.ir.vertex.IRVertex;
import edu.snu.onyx.common.ir.executionproperty.ExecutionProperty;
import edu.snu.onyx.common.ir.vertex.executionproperty.ExecutorPlacementProperty;

/**
 * Pass for initiating IRVertex ExecutorPlacement ExecutionProperty with default values.
 * NONE is the default value.
 */
public final class DefaultVertexExecutorPlacementPass extends AnnotatingPass {
  public static final String SIMPLE_NAME = "DefaultVertexExecutorPlacementPass";

  public DefaultVertexExecutorPlacementPass() {
    super(ExecutionProperty.Key.ExecutorPlacement);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.topologicalDo(irVertex -> {
      if (irVertex.getProperty(ExecutionProperty.Key.ExecutorPlacement) == null) {
        irVertex.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.NONE));
      }
    });
    return dag;
  }
}
