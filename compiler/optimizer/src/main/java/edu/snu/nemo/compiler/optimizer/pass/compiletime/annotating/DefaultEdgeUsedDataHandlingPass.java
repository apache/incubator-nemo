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
import edu.snu.nemo.common.ir.edge.executionproperty.InterTaskDataStoreProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.UsedDataHandlingProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;

import java.util.Collections;

/**
 * Pass for initiating IREdge UsedDataHandling ExecutionProperty with default values.
 */
public final class DefaultEdgeUsedDataHandlingPass extends AnnotatingPass {

  /**
   * Default constructor.
   */
  public DefaultEdgeUsedDataHandlingPass() {
    super(UsedDataHandlingProperty.class, Collections.singleton(InterTaskDataStoreProperty.class));
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.topologicalDo(irVertex ->
        dag.getIncomingEdgesOf(irVertex).forEach(irEdge -> {
          if (!irEdge.getPropertyValue(UsedDataHandlingProperty.class).isPresent()) {
            final InterTaskDataStoreProperty.Value dataStoreValue
                = irEdge.getPropertyValue(InterTaskDataStoreProperty.class).get();
            if (InterTaskDataStoreProperty.Value.MemoryStore.equals(dataStoreValue)
                || InterTaskDataStoreProperty.Value.SerializedMemoryStore.equals(dataStoreValue)) {
              irEdge.setProperty(UsedDataHandlingProperty.of(UsedDataHandlingProperty.Value.Discard));
            } else {
              irEdge.setProperty(UsedDataHandlingProperty.of(UsedDataHandlingProperty.Value.Keep));
            }
          }
        }));
    return dag;
  }
}
