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
package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DataFlowProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DataPersistenceProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourceSlotProperty;
import org.apache.nemo.common.ir.vertex.utility.RelayVertex;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;

/**
 * This pass assumes that a RelayVertex was previously inserted to receive each shuffle edge.
 * <p>
 * src - shuffle-edge - streamvertex - one-to-one-edge - dst
 * <p>
 * (1) shuffle-edge
 * Encode/compress into byte[], and have the receiver read data as the same byte[], rather than decompressing/decoding.
 * Perform a push-based in-memory shuffle with discarding on.
 * <p>
 * (2) streamvertex
 * Ignore resource slots, such that all tasks fetch the in-memory input data blocks as soon as they become available.
 * <p>
 * (3) one-to-one-edge
 * Do not encode/compress the byte[]
 * Perform a pull-based and on-disk data transfer with the DedicatedKeyPerElement.
 */
@Annotates({DataFlowProperty.class, DataPersistenceProperty.class, DataStoreProperty.class, ResourceSlotProperty.class})
@Requires(CommunicationPatternProperty.class)
public final class LargeShuffleAnnotatingPass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public LargeShuffleAnnotatingPass() {
    super(LargeShuffleAnnotatingPass.class);
  }

  @Override
  public IRDAG apply(final IRDAG dag) {
    dag.topologicalDo(irVertex ->
      dag.getIncomingEdgesOf(irVertex).forEach(edge -> {
        if (edge.getDst().getClass().equals(RelayVertex.class)) {
          // CASE #1: To a stream vertex

          // Data transfers
          edge.setPropertyPermanently(DataFlowProperty.of(DataFlowProperty.Value.PUSH));
          edge.setPropertyPermanently(DataPersistenceProperty.of(DataPersistenceProperty.Value.DISCARD));
          edge.setPropertyPermanently(DataStoreProperty.of(DataStoreProperty.Value.SERIALIZED_MEMORY_STORE));

          // Resource slots
          edge.getDst().setPropertyPermanently(ResourceSlotProperty.of(false));
        } else if (edge.getSrc().getClass().equals(RelayVertex.class)) {
          // CASE #2: From a stream vertex

          // Data transfers
          edge.setPropertyPermanently(DataFlowProperty.of(DataFlowProperty.Value.PULL));
          edge.setPropertyPermanently(DataStoreProperty.of(DataStoreProperty.Value.LOCAL_FILE_STORE));
        }
      }));
    return dag;
  }
}
