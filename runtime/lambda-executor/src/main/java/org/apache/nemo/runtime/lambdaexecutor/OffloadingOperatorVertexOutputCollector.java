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
package org.apache.nemo.runtime.lambdaexecutor;

import org.apache.nemo.common.NextIntraTaskOperatorInfo;
import org.apache.nemo.common.TimestampAndValue;
import org.apache.nemo.common.Triple;
import org.apache.nemo.common.dag.Edge;
import org.apache.nemo.common.ir.AbstractOutputCollector;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.punctuation.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
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
  private final Map<String, NextIntraTaskOperatorInfo> internalMainOutputs;
  private final List<NextIntraTaskOperatorInfo> nextOperators;
  private final Map<String, List<NextIntraTaskOperatorInfo>> internalAdditionalOutputs;

  private final OffloadingResultCollector resultCollector;
  private final Edge edge;
  private final Map<String, OffloadingOperatorVertexOutputCollector> outputCollectorMap;

  /**
   * Constructor of the output collector.
   * @param irVertex the ir vertex that emits the output
   * @param internalAdditionalOutputs internal additional outputs
   */
  public OffloadingOperatorVertexOutputCollector(
    final IRVertex irVertex,
    final Edge edge,
    final List<NextIntraTaskOperatorInfo> nextOperators,
    final Map<String, List<NextIntraTaskOperatorInfo>> internalAdditionalOutputs,
    final OffloadingResultCollector resultCollector,
    final Map<String, OffloadingOperatorVertexOutputCollector> outputCollectorMap) {
    this.irVertex = irVertex;
    this.edge = edge;
    this.internalMainOutputs = new HashMap<>();
    this.nextOperators = nextOperators;
    for (final NextIntraTaskOperatorInfo info : nextOperators) {
      internalMainOutputs.put(info.getNextOperator().getId(), info);
    }

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
    List<String> nextOpIds = null;

    for (final NextIntraTaskOperatorInfo internalVertex : nextOperators) {
      LOG.info(internalVertex.getNextOperator().getId());
      if (internalVertex.getNextOperator().isSink || !internalVertex.getNextOperator().isOffloading) {
        if (nextOpIds == null) {
          nextOpIds = new LinkedList<>();
        }
        nextOpIds.add(internalVertex.getNextOperator().getId());
      } else {
        //System.out.print(internalVertex.getNextOperator().getId() + ", ");
        final OffloadingOperatorVertexOutputCollector oc =
          outputCollectorMap.get(internalVertex.getNextOperator().getId());
        oc.inputTimestamp = inputTimestamp;
        emit(internalVertex.getNextOperator(), output);
      }
    }

    if (nextOpIds != null) {
      //System.out.println("Emit to resultCollector in " + irVertex.getId());
      resultCollector.result.add(new Triple<>(
        nextOpIds,
        edge.getId(),
        new TimestampAndValue(inputTimestamp, output)));
    }


    // TODO: handle output writer!!
  }

  @Override
  public <T> void emit(final String dstVertexId, final T output) {
    //LOG.info("{} emits {} to {}", irVertex.getId(), output, dstVertexId);
    List<String> nextOpIds = null;

    if (internalAdditionalOutputs.containsKey(dstVertexId)) {
      for (final NextIntraTaskOperatorInfo internalVertex : internalAdditionalOutputs.get(dstVertexId)) {
        if (internalVertex.getNextOperator().isSink || !internalVertex.getNextOperator().isOffloading) {
          if (nextOpIds == null) {
            nextOpIds = new LinkedList<>();
          }
          nextOpIds.add(internalVertex.getNextOperator().getId());
        } else {
          //System.out.print(internalVertex.getNextOperator().getId() + ", ");
          outputCollectorMap.get(internalVertex.getNextOperator().getId()).inputTimestamp = inputTimestamp;
          emit(internalVertex.getNextOperator(), (O) output);
        }
      }

      if (nextOpIds != null) {
        //System.out.println("Emit to resultCollector in " + irVertex.getId());
        resultCollector.result.add(new Triple<>(
          nextOpIds,
          edge.getId(),
          new TimestampAndValue(inputTimestamp, output)));
      }
    }

    // TODO: handle output writer!!
  }

  @Override
  public void emitWatermark(final Watermark watermark) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("{} emits watermark {}", irVertex.getId(), watermark);
    }

    List<String> nextOpIds = null;

    //System.out.println("Operator " + irVertex.getId() + " emits watermark " + watermark);
    // Emit watermarks to internal vertices
    for (final NextIntraTaskOperatorInfo internalVertex : nextOperators) {
      if (internalVertex.getNextOperator().isSink || !internalVertex.getNextOperator().isOffloading) {
        if (nextOpIds == null) {
          nextOpIds = new LinkedList<>();
        }
        nextOpIds.add(internalVertex.getNextOperator().getId());
        //System.out.println("Operator " + irVertex.getId() + " emits watermark " + watermark);
        //System.out.println("Sink Emit watermark " + watermark);
      } else {
        internalVertex.getWatermarkManager().trackAndEmitWatermarks(internalVertex.getEdgeIndex(), watermark);
      }
    }

    for (final List<NextIntraTaskOperatorInfo> internalVertices : internalAdditionalOutputs.values()) {
      for (final NextIntraTaskOperatorInfo internalVertex : internalVertices) {
        if (internalVertex.getNextOperator().isSink || !internalVertex.getNextOperator().isOffloading) {
          if (nextOpIds == null) {
            nextOpIds = new LinkedList<>();
          }
          nextOpIds.add(internalVertex.getNextOperator().getId());
        } else {
          internalVertex.getWatermarkManager().trackAndEmitWatermarks(internalVertex.getEdgeIndex(), watermark);
        }
      }
    }

    if (nextOpIds != null) {
      resultCollector.result.add(new Triple<>(
        nextOpIds,
        edge.getId(),
        watermark));
    }

    // TODO: handle output writer!!
  }
}
