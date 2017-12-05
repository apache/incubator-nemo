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
import edu.snu.onyx.common.ir.edge.executionproperty.DataFlowModelProperty;
import edu.snu.onyx.common.ir.edge.executionproperty.DataStoreProperty;
import edu.snu.onyx.common.ir.edge.executionproperty.UsedDataHandlingProperty;
import edu.snu.onyx.common.ir.executionproperty.ExecutionProperty;
import edu.snu.onyx.common.ir.vertex.IRVertex;

import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A pass to support Sailfish-like shuffle by tagging edges.
 * This pass handles the UsedDataHandling ExecutionProperty.
 */
public final class SailfishEdgeUsedDataHandlingPass extends AnnotatingPass {

  public SailfishEdgeUsedDataHandlingPass() {
    super(ExecutionProperty.Key.UsedDataHandling, Stream.of(
        ExecutionProperty.Key.DataStore,
        ExecutionProperty.Key.DataFlowModel
    ).collect(Collectors.toSet()));
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.topologicalDo(irVertex ->
        dag.getIncomingEdgesOf(irVertex).forEach(irEdge -> {
          final DataFlowModelProperty.Value dataFlowModel = irEdge.getProperty(ExecutionProperty.Key.DataFlowModel);
          final DataStoreProperty.Value dataStore = irEdge.getProperty(ExecutionProperty.Key.DataStore);
          if (DataFlowModelProperty.Value.Push.equals(dataFlowModel)) {
            irEdge.setProperty(UsedDataHandlingProperty.of(UsedDataHandlingProperty.Value.Discard));
          //} else if (DataStoreProperty.Value.LocalFileStore.equals(dataStore)
           //   || DataStoreProperty.Value.GlusterFileStore.equals(dataStore)) {
           // irEdge.setProperty(UsedDataHandlingProperty.of(UsedDataHandlingProperty.Value.Keep));
          }
        }));
    return dag;
  }
}
