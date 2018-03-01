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

import edu.snu.nemo.common.Pair;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.DuplicateDataProperty;
import edu.snu.nemo.common.ir.executionproperty.ExecutionProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;

import java.util.HashMap;

/**
 * A pass for annotate invariant data for each edge.
 */
public final class DuplicateDataMarkingPass extends AnnotatingPass {

  /**
   * Default constructor.
   */
  public DuplicateDataMarkingPass() {
    super(ExecutionProperty.Key.DuplicateData);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    final HashMap<String, Integer> duplicateEdgeIdToNumberOfDuplicates = new HashMap<>();
    dag.topologicalDo(vertex -> dag.getIncomingEdgesOf(vertex)
        .forEach(e -> {
          final Pair<String, Integer> duplicateDataProperty = e.getProperty(ExecutionProperty.Key.DuplicateData);
          if (duplicateDataProperty != null) {
            final String id = duplicateDataProperty.left();
            final Integer currentCount = duplicateEdgeIdToNumberOfDuplicates.getOrDefault(id, 0);
            duplicateEdgeIdToNumberOfDuplicates.put(id, currentCount + 1);
          }
        }));

    dag.topologicalDo(vertex -> dag.getIncomingEdgesOf(vertex)
        .forEach(e -> {
          final Pair<String, Integer> duplicateDataProperty = e.getProperty(ExecutionProperty.Key.DuplicateData);
          if (duplicateDataProperty != null) {
            final String id = duplicateDataProperty.left();
            if (duplicateEdgeIdToNumberOfDuplicates.containsKey(id)) {
              e.setProperty(DuplicateDataProperty.of(Pair.of(id, duplicateEdgeIdToNumberOfDuplicates.get(id))));
            }
          }
        }));

    return dag;
  }
}
