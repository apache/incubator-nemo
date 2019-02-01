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

import org.apache.nemo.common.coder.BytesDecoderFactory;
import org.apache.nemo.common.coder.BytesEncoderFactory;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourceSlotProperty;
import org.apache.nemo.common.ir.vertex.utility.StreamVertex;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;

/**
 * This pass assumes that a StreamVertex was previously inserted to receive each shuffle edge.
 *
 * src - shuffle-edge - streamvertex - one-to-one-edge - dst
 *
 * (1) shuffle-edge
 * Encode/compress into byte[], and have the receiver read data as the same byte[], rather than decompressing/decoding.
 * Perform a push-based in-memory shuffle with discarding on.
 *
 * (2) streamvertex
 * Ignore resource slots, such that all tasks fetch the in-memory input data blocks as soon as they become available.
 *
 * (3) one-to-one-edge
 * Do not encode/compress the byte[]
 * Perform a pull-based and on-disk data transfer with the DedicatedKeyPerElement.
 */
@Annotates({CompressionProperty.class, DataFlowProperty.class, CompressionProperty.class,
  DataPersistenceProperty.class, DataStoreProperty.class, DecoderProperty.class, DecompressionProperty.class,
  EncoderProperty.class, PartitionerProperty.class, ResourceSlotProperty.class})
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
        if (edge.getDst().getClass().equals(StreamVertex.class)) {
          // CASE #1: To a stream vertex

          // Coder and Compression
          edge.setPropertyPermanently(DecoderProperty.of(BytesDecoderFactory.of()));
          edge.setPropertyPermanently(CompressionProperty.of(CompressionProperty.Value.LZ4));
          edge.setPropertyPermanently(DecompressionProperty.of(CompressionProperty.Value.None));

          // Data transfers
          edge.setPropertyPermanently(DataFlowProperty.of(DataFlowProperty.Value.Push));
          edge.setPropertyPermanently(DataPersistenceProperty.of(DataPersistenceProperty.Value.Discard));
          edge.setPropertyPermanently(DataStoreProperty.of(DataStoreProperty.Value.SerializedMemoryStore));

          // Resource slots
          edge.getDst().setPropertyPermanently(ResourceSlotProperty.of(false));
        } else if (edge.getSrc().getClass().equals(StreamVertex.class)) {
          // CASE #2: From a stream vertex

          // Coder and Compression
          edge.setPropertyPermanently(EncoderProperty.of(BytesEncoderFactory.of()));
          edge.setPropertyPermanently(CompressionProperty.of(CompressionProperty.Value.None));
          edge.setPropertyPermanently(DecompressionProperty.of(CompressionProperty.Value.LZ4));

          // Data transfers
          edge.setPropertyPermanently(DataFlowProperty.of(DataFlowProperty.Value.Pull));
          edge.setPropertyPermanently(DataStoreProperty.of(DataStoreProperty.Value.LocalFileStore));
          edge.setPropertyPermanently(
            PartitionerProperty.of(PartitionerProperty.Type.DedicatedKeyPerElement));
        } else {
          // CASE #3: Unrelated to any stream vertices
          edge.setPropertyPermanently(DataFlowProperty.of(DataFlowProperty.Value.Pull));
        }
      }));
    return dag;
  }
}
