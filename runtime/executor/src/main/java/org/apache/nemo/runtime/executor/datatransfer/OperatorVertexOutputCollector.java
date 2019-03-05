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
package org.apache.nemo.runtime.executor.datatransfer;

import org.apache.nemo.common.NextIntraTaskOperatorInfo;
import org.apache.nemo.common.TimestampAndValue;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.punctuation.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * OffloadingOutputCollector implementation.
 * This emits four types of outputs
 * 1) internal main outputs: this output becomes the input of internal Transforms
 * 2) internal additional outputs: this additional output becomes the input of internal Transforms
 * 3) external main outputs: this external output is emitted to OutputWriter
 * 4) external additional outputs: this external output is emitted to OutputWriter
 *
 * @param <O> output type.
 */
public final class OperatorVertexOutputCollector<O> implements OutputCollector<O> {
  private static final Logger LOG = LoggerFactory.getLogger(OperatorVertexOutputCollector.class.getName());

  private static final String BUCKET_NAME = "nemo-serverless";

  private final Map<String, OperatorVertexOutputCollector> outputCollectorMap;
  private final IRVertex irVertex;
  private final List<NextIntraTaskOperatorInfo> internalMainOutputs;
  private final Map<String, List<NextIntraTaskOperatorInfo>> internalAdditionalOutputs;
  private final List<OutputWriter> externalMainOutputs;
  private final Map<String, List<OutputWriter>> externalAdditionalOutputs;

  // for logging
  private long inputTimestamp;

  /**
   * Constructor of the output collector.
   * @param irVertex the ir vertex that emits the output
   * @param internalMainOutputs internal main outputs
   * @param internalAdditionalOutputs internal additional outputs
   * @param externalMainOutputs external main outputs
   * @param externalAdditionalOutputs external additional outputs
   */
  public OperatorVertexOutputCollector(
    final Map<String, OperatorVertexOutputCollector> outputCollectorMap,
    final IRVertex irVertex,
    final List<NextIntraTaskOperatorInfo> internalMainOutputs,
    final Map<String, List<NextIntraTaskOperatorInfo>> internalAdditionalOutputs,
    final List<OutputWriter> externalMainOutputs,
    final Map<String, List<OutputWriter>> externalAdditionalOutputs) {
    this.outputCollectorMap = outputCollectorMap;
    this.irVertex = irVertex;
    this.internalMainOutputs = internalMainOutputs;
    this.internalAdditionalOutputs = internalAdditionalOutputs;
    this.externalMainOutputs = externalMainOutputs;
    this.externalAdditionalOutputs = externalAdditionalOutputs;
  }

  private void emit(final OperatorVertex vertex, final O output) {
    final String vertexId = irVertex.getId();
    vertex.getTransform().onData(output);
  }

  private void emit(final OutputWriter writer, final TimestampAndValue<O> output) {
    // metric collection
    //LOG.info("Write {} to writer {}", output, writer);

    final String vertexId = irVertex.getId();
    writer.write(output);
  }

  @Override
  public void setTimestamp(long ts) {
    inputTimestamp = ts;
  }

  @Override
  public long getTimestamp() {
    return inputTimestamp;
  }

  @Override
  public void emit(final O output) {
    //LOG.info("{} emits {} / timestamp: {}", irVertex.getId(), output, inputTimestamp);

    if (irVertex.isSink) {
      // metric collection
    }

    for (final NextIntraTaskOperatorInfo internalVertex : internalMainOutputs) {
      final OperatorVertexOutputCollector oc = outputCollectorMap.get(internalVertex.getNextOperator().getId());
      oc.inputTimestamp = inputTimestamp;
      emit(internalVertex.getNextOperator(), output);
    }

    for (final OutputWriter externalWriter : externalMainOutputs) {
      emit(externalWriter, new TimestampAndValue<>(inputTimestamp, output));
    }
  }

  @Override
  public <T> void emit(final String dstVertexId, final T output) {
    //LOG.info("{} emits {} to {}", irVertex.getId(), output, dstVertexId);

    if (internalAdditionalOutputs.containsKey(dstVertexId)) {
      for (final NextIntraTaskOperatorInfo internalVertex : internalAdditionalOutputs.get(dstVertexId)) {
        final OperatorVertexOutputCollector oc = outputCollectorMap.get(internalVertex.getNextOperator().getId());
        oc.inputTimestamp = inputTimestamp;
        emit(internalVertex.getNextOperator(), (O) output);
      }
    }


    if (externalAdditionalOutputs.containsKey(dstVertexId)) {
      for (final OutputWriter externalWriter : externalAdditionalOutputs.get(dstVertexId)) {
        emit(externalWriter, new TimestampAndValue<>(inputTimestamp, (O) output));
      }
    }
  }

  @Override
  public void emitWatermark(final Watermark watermark) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("{} emits watermark {}", irVertex.getId(), watermark);
    }

    // Emit watermarks to internal vertices
    for (final NextIntraTaskOperatorInfo internalVertex : internalMainOutputs) {
      internalVertex.getWatermarkManager().trackAndEmitWatermarks(internalVertex.getEdgeIndex(), watermark);
    }

    for (final List<NextIntraTaskOperatorInfo> internalVertices : internalAdditionalOutputs.values()) {
      for (final NextIntraTaskOperatorInfo internalVertex : internalVertices) {
        internalVertex.getWatermarkManager().trackAndEmitWatermarks(internalVertex.getEdgeIndex(), watermark);
      }
    }

    // QUERY7
    /*
    if (irVertex.getId().equals("vertex15")) {
      sideInputOutputCollector.emitWatermark(watermark);
    } else if (irVertex.getId().equals("vertex6")) {
      mainInputLambdaCollector.emitWatermark(watermark);
    } else {
      // Emit watermarks to output writer
      for (final OutputWriter outputWriter : externalMainOutputs) {
        outputWriter.writeWatermark(watermark);
      }

      for (final List<OutputWriter> externalVertices : externalAdditionalOutputs.values()) {
        for (final OutputWriter externalVertex : externalVertices) {
          externalVertex.writeWatermark(watermark);
        }
      }
    }
    */

    // Emit watermarks to output writer
    for (final OutputWriter outputWriter : externalMainOutputs) {
      outputWriter.writeWatermark(watermark);
    }

    for (final List<OutputWriter> externalVertices : externalAdditionalOutputs.values()) {
      for (final OutputWriter externalVertex : externalVertices) {
        externalVertex.writeWatermark(watermark);
      }
    }
  }
}
