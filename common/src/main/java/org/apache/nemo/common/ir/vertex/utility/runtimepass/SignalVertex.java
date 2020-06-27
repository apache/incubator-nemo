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
package org.apache.nemo.common.ir.vertex.utility.runtimepass;

import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.MessageIdVertexProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.transform.SignalTransform;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Signal vertex holding signal transform.
 * It triggers runtime pass without examining related edge's data.
 *
 * e.g) suppose that we want to change vertex 2's property by using runtime pass, but the related data is not gained
 * directly from the incoming edge of vertex 2 (for example, the data is gained from using simulation).
 * In this case, it is unnecessary to insert message generator vertex and message aggregator vertex to launch runtime
 * pass.
 *
 * Original case: (vertex1) -- shuffle edge -- (vertex 2)
 *
 * After inserting signal Vertex:
 * (vertex 1) -------------------- shuffle edge ------------------- (vertex 2)
 *            -- control edge -- (signal vertex) -- control edge --
 *
 * Therefore, the shuffle edge to vertex 2 is executed after signal vertex is executed.
 * Since signal vertex only 'signals' the launch of runtime pass, its parallelism is sufficient to be only 1.
 */
public final class SignalVertex extends OperatorVertex {
  private static final AtomicInteger MESSAGE_ID_GENERATOR = new AtomicInteger(0);

  public SignalVertex() {
    super(new SignalTransform());
    this.setPropertyPermanently(MessageIdVertexProperty.of(MESSAGE_ID_GENERATOR.incrementAndGet()));
    this.setPropertyPermanently(ParallelismProperty.of(1));
  }
}
