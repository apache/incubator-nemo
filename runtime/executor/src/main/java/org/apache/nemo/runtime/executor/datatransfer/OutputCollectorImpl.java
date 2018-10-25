/*
 * Copyright (C) 2018 Seoul National University
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
package org.apache.nemo.runtime.executor.datatransfer;

import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * OutputCollector implementation.
 * This emits four types of outputs
 * 1) internal main outputs: this output becomes the input of internal Transforms
 * 2) internal additional outputs: this additional output becomes the input of internal Transforms
 * 3) external main outputs: this external output is emitted to OutputWriter
 * 4) external additional outputs: this external output is emitted to OutputWriter
 *
 * @param <O> output type.
 */
public final class OutputCollectorImpl<O> implements OutputCollector<O> {
  private static final Logger LOG = LoggerFactory.getLogger(OutputCollectorImpl.class.getName());

  private final IRVertex irVertex;
  private final List<OperatorVertex> internalMainOutputs;
  private final Map<String, List<OperatorVertex>> internalAdditionalOutputs;
  private final List<OutputWriter> externalMainOutputs;
  private final Map<String, List<OutputWriter>> externalAdditionalOutputs;

  /**
   * Constructor of the output collector.
   * @param irVertex the ir vertex that emits the output
   * @param internalMainOutputs internal main outputs
   * @param internalAdditionalOutputs internal additional outputs
   * @param externalMainOutputs external main outputs
   * @param externalAdditionalOutputs external additional outputs
   */
  public OutputCollectorImpl(final IRVertex irVertex,
                             final List<OperatorVertex> internalMainOutputs,
                             final Map<String, List<OperatorVertex>> internalAdditionalOutputs,
                             final List<OutputWriter> externalMainOutputs,
                             final Map<String, List<OutputWriter>> externalAdditionalOutputs) {
    this.irVertex = irVertex;
    this.internalMainOutputs = internalMainOutputs;
    this.internalAdditionalOutputs = internalAdditionalOutputs;
    this.externalMainOutputs = externalMainOutputs;
    this.externalAdditionalOutputs = externalAdditionalOutputs;
  }

  private void emit(final OperatorVertex vertex, final O output) {
    vertex.getTransform().onData(output);
  }

  private void emit(final OutputWriter writer, final O output) {
    writer.write(output);
  }

  @Override
  public void emit(final O output) {
    for (final OperatorVertex internalVertex : internalMainOutputs) {
      emit(internalVertex, output);
    }

    for (final OutputWriter externalWriter : externalMainOutputs) {
      emit(externalWriter, output);
    }
  }

  @Override
  public <T> void emit(final String dstVertexId, final T output) {

    if (internalAdditionalOutputs.containsKey(dstVertexId)) {
      for (final OperatorVertex internalVertex : internalAdditionalOutputs.get(dstVertexId)) {
        emit(internalVertex, (O) output);
      }
    }

    if (externalAdditionalOutputs.containsKey(dstVertexId)) {
      for (final OutputWriter externalWriter : externalAdditionalOutputs.get(dstVertexId)) {
        emit(externalWriter, (O) output);
      }
    }
  }
}
