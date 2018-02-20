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
package edu.snu.coral.compiler.optimizer.pass.compiletime.annotating;

import edu.snu.coral.common.Pair;
import edu.snu.coral.common.dag.DAG;
import edu.snu.coral.common.ir.edge.IREdge;
import edu.snu.coral.common.ir.edge.executionproperty.InvariantDataProperty;
import edu.snu.coral.common.ir.executionproperty.ExecutionProperty;
import edu.snu.coral.common.ir.vertex.IRVertex;

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
    final HashMap<String, String> realIdMapping = new HashMap<>();
    dag.topologicalDo(vertex -> dag.getIncomingEdgesOf(vertex)
        .forEach(e -> {
          final Pair<Boolean, String> invariantDataProperty = vertex.getProperty(ExecutionProperty.Key.InvariantData);
          if (invariantDataProperty != null) {
            realIdMapping.putIfAbsent(invariantDataProperty.right(), vertex.getId());
            vertex.setProperty(InvariantDataProperty.of(Pair.of(Boolean.TRUE, vertex.getId())));
          }
        }));
    return dag;
  }
}
