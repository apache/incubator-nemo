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

import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.vertex.SourceVertex;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Executes the original IRVertex using a randomly sampled subset of input data partitions.
 */
public final class SamplingSourceVertex<T> extends SourceVertex<T> {
  private SourceVertex<T> originalVertex;
  private float sampleRate;
  private final Random random; // for deterministic getReadables()

  /**
   * Constructor.
   */
  public SamplingSourceVertex(final SourceVertex<T> originalVertex,
                              final float sampleRate) {
    this.originalVertex = originalVertex;
    this.sampleRate = sampleRate;
    this.random = new Random();
  }

  @Override
  public SamplingSourceVertex getClone() {
    final SamplingSourceVertex that = new SamplingSourceVertex<>(this.originalVertex, this.sampleRate);
    this.copyExecutionPropertiesTo(that);
    return that;
  }

  @Override
  public boolean isBounded() {
    return originalVertex.isBounded();
  }

  @Override
  public List<Readable<T>> getReadables(final int desiredNumOfSplits) throws Exception {
    final List<Readable<T>> originalReadables = originalVertex.getReadables(desiredNumOfSplits);
    final int numOfSampledReadables = (int) Math.ceil(originalReadables.size() *  sampleRate);

    // Random indices
    final List<Integer> randomIndices =
      IntStream.range(0, originalReadables.size()).boxed().collect(Collectors.toList());
    Collections.shuffle(randomIndices, random);

    // Return the selected readables
    final List<Integer> indicesToSample = randomIndices.subList(0, numOfSampledReadables);
    return indicesToSample.stream().map(originalReadables::get).collect(Collectors.toList());
  }

  @Override
  public void clearInternalStates() {
    originalVertex.clearInternalStates();
  }

  public SourceVertex<T> getOriginalVertex() {
    return originalVertex;
  }
}
