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

import java.util.ArrayList;
import java.util.List;

/**
 * A wrapper source vertex for a sampled bounded source vertex.
 * @param <T> the type of data to emit.
 */
public final class SampledSourceVertex<T> extends SourceVertex<T> {
  private SourceVertex<T> sourceVertexToSample;
  private List<Integer> idxToSample;
  private int originalParallelism;

  /**
   * Constructor.
   */
  public SampledSourceVertex(final SourceVertex<T> sourceVertexToSample,
                             final List<Integer> idxToSample,
                             final int originalParallelism) {
    this.sourceVertexToSample = sourceVertexToSample;
    this.idxToSample = idxToSample;
    this.originalParallelism = originalParallelism;
  }

  @Override
  public IRVertex getSampledClone(final List<Integer> idxToSampleToSet,
                                  final int originalParallelism) {
    throw new RuntimeException("Cannot sample twice!");
  }

  @Override
  public SampledSourceVertex getClone() {
    final SampledSourceVertex that =
        new SampledSourceVertex<>(this.sourceVertexToSample, this.idxToSample, this.originalParallelism);
    this.copyExecutionPropertiesTo(that);
    return that;
  }

  @Override
  public List<Readable<T>> getReadables(final int desiredNumOfSplits) throws Exception {
    final List<Readable<T>> readables = sourceVertexToSample.getReadables(originalParallelism);
    if (readables.size() < desiredNumOfSplits
        || desiredNumOfSplits != idxToSample.size()) {
      throw new RuntimeException("Sampled size mismatch!");
    }

    final List<Readable<T>> sampledReadables = new ArrayList<>();
    for (final Integer idx : idxToSample) {
      sampledReadables.add(readables.get(idx));
    }

    return sampledReadables;
  }

  @Override
  public void clearInternalStates() {
    sourceVertexToSample = null;
    idxToSample = null;
    originalParallelism = -1;
  }
}
