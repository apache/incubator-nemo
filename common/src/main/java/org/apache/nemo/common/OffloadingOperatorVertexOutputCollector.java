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
package org.apache.nemo.common;

import org.apache.nemo.common.dag.Edge;
import org.apache.nemo.common.eventhandler.OffloadingDataEvent;
import org.apache.nemo.common.eventhandler.OffloadingResultEvent;
import org.apache.nemo.common.ir.AbstractOutputCollector;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.offloading.common.OffloadingOutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
public final class OffloadingOperatorVertexOutputCollector<O> extends AbstractOutputCollector<O> {
  private static final Logger LOG = LoggerFactory.getLogger(OffloadingOperatorVertexOutputCollector.class.getName());

  private static final String BUCKET_NAME = "nemo-serverless";

  private final IRVertex irVertex;
  private final List<NextIntraTaskOperatorInfo> internalMainOutputs;
  private final Map<String, List<NextIntraTaskOperatorInfo>> internalAdditionalOutputs;

  private final OffloadingResultCollector resultCollector;
  private final Edge edge;
  private final Map<String, OffloadingOperatorVertexOutputCollector> outputCollectorMap;

  /**
   * Constructor of the output collector.
   * @param irVertex the ir vertex that emits the output
   * @param internalMainOutputs internal main outputs
   * @param internalAdditionalOutputs internal additional outputs
   */
  public OffloadingOperatorVertexOutputCollector(
    final IRVertex irVertex,
    final Edge edge,
    final List<NextIntraTaskOperatorInfo> internalMainOutputs,
    final Map<String, List<NextIntraTaskOperatorInfo>> internalAdditionalOutputs,
    final OffloadingResultCollector resultCollector,
    final Map<String, OffloadingOperatorVertexOutputCollector> outputCollectorMap) {
    this.irVertex = irVertex;
    this.edge = edge;
    this.internalMainOutputs = internalMainOutputs;
    this.internalAdditionalOutputs = internalAdditionalOutputs;
    this.resultCollector = resultCollector;
    this.outputCollectorMap = outputCollectorMap;
  }

  private void emit(final OperatorVertex vertex, final O output) {
    vertex.getTransform().onData(output);
  }

  @Override
  public void emit(final O output) {
    LOG.info("Operator " + irVertex.getId() + " emit " + output + " to ");
    for (final NextIntraTaskOperatorInfo internalVertex : internalMainOutputs) {
      LOG.info(internalVertex.getNextOperator().getId());
      if (internalVertex.getNextOperator().isSink) {
        //System.out.println("Emit to resultCollector in " + irVertex.getId());
        resultCollector.result.add(new Triple<>(
          irVertex.getId(),
          edge.getId(),
          new TimestampAndValue(inputTimestamp, output)));
      } else {
        //System.out.print(internalVertex.getNextOperator().getId() + ", ");
        final OffloadingOperatorVertexOutputCollector oc =
          outputCollectorMap.get(internalVertex.getNextOperator().getId());
        oc.inputTimestamp = inputTimestamp;
        emit(internalVertex.getNextOperator(), output);
      }
    }
  }

  @Override
  public <T> void emit(final String dstVertexId, final T output) {
    //LOG.info("{} emits {} to {}", irVertex.getId(), output, dstVertexId);

    if (internalAdditionalOutputs.containsKey(dstVertexId)) {
      for (final NextIntraTaskOperatorInfo internalVertex : internalAdditionalOutputs.get(dstVertexId)) {
        if (internalVertex.getNextOperator().isSink) {
          //System.out.println("Emit to resultCollector in " + irVertex.getId());
          resultCollector.result.add(new Triple<>(
            irVertex.getId(),
            edge.getId(),
            new TimestampAndValue(inputTimestamp, output)));
        } else {
          //System.out.print(internalVertex.getNextOperator().getId() + ", ");
          outputCollectorMap.get(internalVertex.getNextOperator().getId()).inputTimestamp = inputTimestamp;
          emit(internalVertex.getNextOperator(), (O) output);
        }
      }
    } else {
      // TODO: do sth here !! this should be sent to the vm
      throw new RuntimeException("Not support multi output in serverless: " + dstVertexId + ", vertex: " + irVertex);
      //resultCollector.result.add(Pair.of(output, dstVertexId));
    }
  }

  @Override
  public void emitWatermark(final Watermark watermark) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("{} emits watermark {}", irVertex.getId(), watermark);
    }

    //System.out.println("Operator " + irVertex.getId() + " emits watermark " + watermark);
    // Emit watermarks to internal vertices
    for (final NextIntraTaskOperatorInfo internalVertex : internalMainOutputs) {
      if (internalVertex.getNextOperator().isSink) {
        //System.out.println("Sink Emit watermark " + watermark);
        resultCollector.result.add(new Triple<>(
          irVertex.getId(),
          edge.getId(),
          watermark));
      } else {
        internalVertex.getWatermarkManager().trackAndEmitWatermarks(internalVertex.getEdgeIndex(), watermark);
      }
    }

    for (final List<NextIntraTaskOperatorInfo> internalVertices : internalAdditionalOutputs.values()) {
      for (final NextIntraTaskOperatorInfo internalVertex : internalVertices) {
        internalVertex.getWatermarkManager().trackAndEmitWatermarks(internalVertex.getEdgeIndex(), watermark);
      }
    }
  }
}
