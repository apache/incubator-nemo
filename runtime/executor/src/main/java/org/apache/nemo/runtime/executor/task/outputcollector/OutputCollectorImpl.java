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
package org.apache.nemo.runtime.executor.task.outputcollector;

import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.runtime.executor.datatransfer.OutputWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * OutputCollector implementation.
 *
 * @param <O> output type.
 */
public final class OutputCollectorImpl<O> implements OutputCollector<O> {
  private static final Logger LOG = LoggerFactory.getLogger(OutputCollectorImpl.class.getName());

  private final IRVertex irVertex;
  private final Set<OperatorVertex> internalMainOutputs;
  private final Map<String, Set<OperatorVertex>> internalAdditionalOutputs;
  private final Set<OutputWriter> externalMainOutputs;
  private final Map<String, Set<OutputWriter>> externalAdditionalOutputs;

  /**
   * Constructor of a new OutputCollectorImpl with tagged outputs.
   * @param tagToChildren additional children vertices
   */
  public OutputCollectorImpl(final IRVertex irVertex,
                             final Set<OperatorVertex> internalMainOutputs,
                             final Map<String, Set<OperatorVertex>> internalAdditionalOutputs,
                             final Set<OutputWriter> externalMainOutputs,
                             final Map<String, Set<OutputWriter>> externalAdditionalOutputs) {
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
