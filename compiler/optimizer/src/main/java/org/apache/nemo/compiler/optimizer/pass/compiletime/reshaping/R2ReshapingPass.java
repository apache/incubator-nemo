/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.compiler.optimizer.pass.compiletime.reshaping;

import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.transform.CRTransform;
import org.apache.nemo.common.ir.vertex.transform.SrcCRTransform;
import org.apache.nemo.common.ir.vertex.utility.ConditionalRouterVertex;
import org.apache.nemo.common.ir.vertex.utility.SrcStreamVertex;
import org.apache.nemo.common.ir.vertex.utility.StreamVertex;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;

import java.util.LinkedList;
import java.util.List;

/**
 * Inserts the StreamVertex for each shuffle edge.
 */
@Requires(CommunicationPatternProperty.class)
public final class R2ReshapingPass extends ReshapingPass {

  /**
   * Default constructor.
   */
  public R2ReshapingPass() {
    super(R2ReshapingPass.class);
  }


  @Override
  public IRDAG apply(final IRDAG dag) {

    // 1. add router vertex
    dag.topologicalDo(vertex -> {
      final List<IREdge> edges = dag.getIncomingEdgesOf(vertex);

      if (vertex instanceof SrcStreamVertex) {
        // Local conditional router vertex
        dag.change((OperatorVertex) vertex, new ConditionalRouterVertex(new CRTransform()));
      }

      if (vertex instanceof StreamVertex) {
        dag.change((OperatorVertex) vertex, new ConditionalRouterVertex(new CRTransform()));
      }
    });

    // 2. add transient data path
    dag.topologicalDo(vertex -> {
      if (vertex instanceof ConditionalRouterVertex) {
        final List<IRVertex> verticesToAdd = new LinkedList<>();
        final List<IRVertex> stack = new LinkedList<>();

        stack.add(vertex);
        while (!stack.isEmpty()) {
          final IRVertex s = ((LinkedList<IRVertex>) stack).poll();
          final List<IREdge> outEdges = dag.getOutgoingEdgesOf(s);
          for (final IREdge outEdge : outEdges) {
            if (!(outEdge.getDst() instanceof ConditionalRouterVertex)) {
              if (!verticesToAdd.contains(outEdge.getDst())) {
                if (!outEdge.getDst().isAddedInDriver()) {
                  verticesToAdd.add(outEdge.getDst());
                  stack.add(outEdge.getDst());
                }
              }
            }
          }
        }

        dag.addTransientDataPath(
          (ConditionalRouterVertex) vertex,
          verticesToAdd);
      }
    });


    return dag;
  }
}
