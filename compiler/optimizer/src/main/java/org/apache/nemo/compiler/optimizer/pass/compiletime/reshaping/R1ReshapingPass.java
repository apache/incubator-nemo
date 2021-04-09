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
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.ir.vertex.utility.SrcStreamVertex;
import org.apache.nemo.common.ir.vertex.utility.StreamVertex;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;

import java.util.List;

/**
 * Inserts the StreamVertex for each shuffle edge.
 */
@Requires(CommunicationPatternProperty.class)
public final class R1ReshapingPass extends ReshapingPass {

  /**
   * Default constructor.
   */
  public R1ReshapingPass() {
    super(R1ReshapingPass.class);
  }


  @Override
  public IRDAG apply(final IRDAG dag) {
    dag.topologicalDo(vertex -> {
      final List<IREdge> edges = dag.getIncomingEdgesOf(vertex);
      final int o2ocount = (int) edges.stream().filter(edge -> CommunicationPatternProperty.Value.OneToOne
        .equals(edge.getPropertyValue(CommunicationPatternProperty.class).get()))
        .count();

      if (o2ocount > 1 && edges.size() == o2ocount) {
        // join !!
        dag.insert(new StreamVertex(), edges);
        //for (final IREdge edge : edges) {
        //  dag.insert(new StreamVertex(), edge);
        // }
      }


      dag.getIncomingEdgesOf(vertex).forEach(edge -> {
        if (CommunicationPatternProperty.Value.Shuffle
          .equals(edge.getPropertyValue(CommunicationPatternProperty.class).get())) {
          dag.insert(new StreamVertex(), edge);
        }

        if (edge.getSrc() instanceof SourceVertex) {
          dag.insert(new SrcStreamVertex(), edge);
        }
      });
    });

    return dag;
  }
}
