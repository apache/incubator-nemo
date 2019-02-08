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
package org.apache.nemo.common.ir.vertex.utility;

import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;

import java.util.Set;

/**
 * Executes the original IRVertex using a subset of input data partitions.
 */
public final class SamplingVertex extends IRVertex {
  final IRVertex originalVertex;
  final float sampleRate;

  /**
   * Constructor.
   */
  public SamplingVertex(final IRVertex originalVertex, final float sampleRate) {
    if (originalVertex instanceof SamplingVertex) {
      throw new IllegalArgumentException("Cannot sample again: " + originalVertex.toString());
    }
    this.originalVertex = originalVertex;
    this.sampleRate = sampleRate;


    if (!edgeToVtxToSample.getPropertyValue(DuplicateEdgeGroupProperty.class).isPresent()) {
      final DuplicateEdgeGroupPropertyValue value =
        new DuplicateEdgeGroupPropertyValue(String.valueOf(duplicateId.getAndIncrement()));
      edgeToVtxToSample.setPropertyPermanently(DuplicateEdgeGroupProperty.of(value));
    }


    final int sampledParallelism = Math.max(Math.round(originalParallelism * sampleRate), 1);
    final List<Integer> randomIndices =
      IntStream.range(0, originalParallelism).boxed().collect(Collectors.toList());
    Collections.shuffle(randomIndices, new Random(System.currentTimeMillis()));
    final List<Integer> idxToSample = randomIndices.subList(0, sampledParallelism);


    final int sampledParallelism = idxToSample.size();
    final IRVertex sampledVtx = vtxToSample instanceof SourceVertex ?
      ((SourceVertex) vtxToSample).getSampledClone(idxToSample, originalParallelism) : vtxToSample.getClone();
    vtxToSample.copyExecutionPropertiesTo(sampledVtx);
    sampledVtx.setPropertyPermanently(ParallelismProperty.of(sampledParallelism));
  }

  public IRVertex getOriginalVertex() {
    return originalVertex;
  }

  public Set<IREdge> getOutgoingEdgesToOriginalDestinations() {
  }

  public float getSampleRate() {
    return sampleRate;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("SamplingVertex(sampleRate:");
    sb.append(String.valueOf(sampleRate));
    sb.append(")[");
    sb.append(originalVertex.toString());
    sb.append("]");
    return sb.toString();
  }
}
