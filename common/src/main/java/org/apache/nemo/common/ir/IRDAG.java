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
package org.apache.nemo.common.ir;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.LoopVertex;
import org.apache.nemo.common.ir.vertex.system.StreamVertex;

import java.util.Map;
import java.util.Set;

public class IRDAG extends DAG<IRVertex, IREdge> {
  public IRDAG(final Set<IRVertex> vertices,
               final Map<IRVertex, Set<IREdge>> incomingEdges,
               final Map<IRVertex, Set<IREdge>> outgoingEdges,
               final Map<IRVertex, LoopVertex> assignedLoopVertexMap,
               final Map<IRVertex, Integer> loopStackDepthMap) {
    super(vertices, incomingEdges, outgoingEdges, assignedLoopVertexMap, loopStackDepthMap);
  }

  void insert(final StreamVertex streamVertex, final IREdge edgeToReplace) {
  }


}
