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
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.executionproperty.ResourceSlotProperty;

import java.util.Collections;

/**
 * Sets {@link ResourceSlotProperty}.
 */
public final class LargeShuffleResourceSlotPass extends AnnotatingPass {

  public LargeShuffleResourceSlotPass() {
    super(ResourceSlotProperty.class, Collections.singleton(DataFlowProperty.class));
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    // On every vertex that receive push edge, if ResourceSlotProperty is not set, put it as false.
    // For other vertices, if ResourceSlotProperty is not set, put it as true.
    dag.getVertices().stream()
        .filter(v -> !v.getExecutionProperties().containsKey(ResourceSlotProperty.class))
        .forEach(v -> {
          if (dag.getIncomingEdgesOf(v).stream().anyMatch(
              e -> e.getPropertyValue(DataFlowProperty.class)
                  .orElseThrow(() -> new RuntimeException(String.format("DataFlowProperty for %s must be set",
                      e.getId()))).equals(DataFlowProperty.Value.Push))) {
            v.getExecutionProperties().put(ResourceSlotProperty.of(false));
          } else {
            v.getExecutionProperties().put(ResourceSlotProperty.of(true));
          }
        });
    return dag;
  }
}
