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
package edu.snu.vortex.compiler.optimizer.passes;

import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.attribute.Attribute;

import java.util.List;

/**
 * Pass which enables I-File style write optimization.
 * It sets IFileWrite attribute on ScatterGather edges with RemoteFile partition store.
 */
public final class IFilePass implements StaticOptimizationPass {
  @Override
  public DAG<IRVertex, IREdge> process(final DAG<IRVertex, IREdge> dag) throws Exception {
    dag.getVertices().forEach(vertex -> {
      final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
      inEdges.forEach(edge -> {
        if (edge.getType().equals(IREdge.Type.ScatterGather)
            && edge.getAttr(Attribute.Key.ChannelDataPlacement).equals(Attribute.RemoteFile)) {
          edge.setAttr(Attribute.Key.WriteOptimization, Attribute.IFileWrite);
        }
      });
    });
    return dag;
  }
}
