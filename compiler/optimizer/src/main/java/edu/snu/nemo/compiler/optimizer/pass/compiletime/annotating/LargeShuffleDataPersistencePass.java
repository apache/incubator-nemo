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
import edu.snu.nemo.common.ir.edge.executionproperty.DataFlowProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.DataPersistenceProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;

import java.util.Collections;

/**
 * A pass to optimize large shuffle by tagging edges.
 * This pass handles the data persistence ExecutionProperty.
 */
public final class LargeShuffleDataPersistencePass extends AnnotatingPass {

  /**
   * Default constructor.
   */
  public LargeShuffleDataPersistencePass() {
    super(DataPersistenceProperty.class, Collections.singleton(DataFlowProperty.class));
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.topologicalDo(irVertex ->
        dag.getIncomingEdgesOf(irVertex).forEach(irEdge -> {
          final DataFlowProperty.Value dataFlowModel = irEdge.getPropertyValue(DataFlowProperty.class).get();
          if (DataFlowProperty.Value.Push.equals(dataFlowModel)) {
            irEdge.setProperty(DataPersistenceProperty.of(DataPersistenceProperty.Value.Discard));
          }
        }));
    return dag;
  }
}
