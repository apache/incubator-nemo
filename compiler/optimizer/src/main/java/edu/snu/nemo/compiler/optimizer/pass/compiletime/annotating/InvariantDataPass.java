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
import edu.snu.nemo.common.ir.edge.executionproperty.InvariantDataProperty;
import edu.snu.nemo.common.ir.executionproperty.ExecutionProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;

import java.util.HashMap;

/**
 * A pass for annotate invariant data for each edge.
 */
public final class InvariantDataPass extends AnnotatingPass {

  /**
   * Default constructor.
   */
  public InvariantDataPass() {
    super(ExecutionProperty.Key.InvariantData);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    final HashMap<String, Integer> idToCountMap = new HashMap<>();
    dag.topologicalDo(vertex -> dag.getIncomingEdgesOf(vertex)
        .forEach(e -> {
          final Pair<Integer, String> invariantDataProperty = e.getProperty(ExecutionProperty.Key.InvariantData);
          if (invariantDataProperty != null) {
            final String id = invariantDataProperty.right();
            final Integer currentCount = idToCountMap.getOrDefault(id, 0);
            idToCountMap.put(id, currentCount + 1);
          }
        }));

    dag.topologicalDo(vertex -> dag.getIncomingEdgesOf(vertex)
        .forEach(e -> {
          final Pair<Integer, String> invariantDataProperty = e.getProperty(ExecutionProperty.Key.InvariantData);
          if (invariantDataProperty != null) {
            final String id = invariantDataProperty.right();
            if (idToCountMap.containsKey(id)) {
              e.setProperty(InvariantDataProperty.of(Pair.of(idToCountMap.get(id), id)));
            }
          }
        }));

    return dag;
  }
}
