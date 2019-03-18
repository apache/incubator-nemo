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

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.runtime.executor.common.NextIntraTaskOperatorInfo;
import org.apache.nemo.common.punctuation.TimestampAndValue;
import org.apache.nemo.common.ir.AbstractOutputCollector;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.runtime.executor.task.ControlEvent;
import org.apache.nemo.runtime.executor.task.OffloadingContext;
import org.apache.nemo.runtime.executor.task.OffloadingControlEvent;
import org.apache.nemo.runtime.executor.task.OperatorMetricCollector;
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
public final class OperatorVertexOutputCollector<O> extends AbstractOutputCollector<O> {
  private static final Logger LOG = LoggerFactory.getLogger(OperatorVertexOutputCollector.class.getName());

  private final Map<String, Pair<OperatorMetricCollector, OutputCollector>> outputCollectorMap;
  private final IRVertex irVertex;
  private final List<NextIntraTaskOperatorInfo> internalMainOutputs;
  private final Map<String, List<NextIntraTaskOperatorInfo>> internalAdditionalOutputs;
  private final List<OutputWriter> externalMainOutputs;
  private final Map<String, List<OutputWriter>> externalAdditionalOutputs;

  // for logging
  private long inputTimestamp;
  private final OperatorMetricCollector operatorMetricCollector;
  private final Map<Long, Long> prevWatermarkMap;

  private long currWatermark = 0;

  private OffloadingContext currOffloadingContext;
  public final Map<String, Pair<PriorityQueue<Watermark>, PriorityQueue<Watermark>>> expectedWatermarkMap;

  /**
   * Constructor of the output collector.
   * @param irVertex the ir vertex that emits the output
   * @param internalMainOutputs internal main outputs
   * @param internalAdditionalOutputs internal additional outputs
   * @param externalMainOutputs external main outputs
   * @param externalAdditionalOutputs external additional outputs
   */
  public OperatorVertexOutputCollector(
    final Map<String, Pair<OperatorMetricCollector, OutputCollector>> outputCollectorMap,
    final IRVertex irVertex,
    final List<NextIntraTaskOperatorInfo> internalMainOutputs,
    final Map<String, List<NextIntraTaskOperatorInfo>> internalAdditionalOutputs,
    final List<OutputWriter> externalMainOutputs,
    final Map<String, List<OutputWriter>> externalAdditionalOutputs,
    final OperatorMetricCollector operatorMetricCollector,
    final Map<Long, Long> prevWatermarkMap,
    final Map<String, Pair<PriorityQueue<Watermark>, PriorityQueue<Watermark>>> expectedWatermarkMap) {
    this.outputCollectorMap = outputCollectorMap;
    this.irVertex = irVertex;
    this.internalMainOutputs = internalMainOutputs;
    this.internalAdditionalOutputs = internalAdditionalOutputs;
    this.externalMainOutputs = externalMainOutputs;
    this.externalAdditionalOutputs = externalAdditionalOutputs;
    this.operatorMetricCollector = operatorMetricCollector;
    this.prevWatermarkMap = prevWatermarkMap;
    this.expectedWatermarkMap = expectedWatermarkMap;
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
  public void setInputTimestamp(long ts) {
    inputTimestamp = ts;
  }

  @Override
  public long getInputTimestamp() {
    return inputTimestamp;
  }


  public void handleControlMessage(final ControlEvent msg) {
    switch (msg.getControlMessageType()) {
      case FLUSH_LATENCY: {
        //LOG.info("Operator {} flush latency", irVertex.getId());
        operatorMetricCollector.flushLatencies();
        break;
      }
      default: {
        throw new RuntimeException("Unsupported type: " + msg.getControlMessageType());
      }
    }
  }


  public void handleOffloadingControlMessage(final OffloadingControlEvent msg) {
    switch (msg.getControlMessageType()) {
      case START_OFFLOADING: {
        LOG.info("Operator {} start to offload", irVertex.getId());
        currOffloadingContext = (OffloadingContext) msg.getData().get();
        operatorMetricCollector.startOffloading();
        offloading = true;
        break;
      }
      case STOP_OFFLOADING: {
        LOG.info("Operator {} end to offload", irVertex.getId());
        offloading = false;
        operatorMetricCollector.endOffloading();
        currOffloadingContext = null;
        break;
      }
      case FLUSH: {
        //LOG.info("Operator {} flush data", irVertex.getId());
        if (operatorMetricCollector.hasFlushableData()) {
          operatorMetricCollector.flushToServerless();
        }
        break;
      }
      default:
        throw new RuntimeException("Unsupported type: " + msg.getControlMessageType());
    }
  }

  @Override
  public void emit(final O output) {
    LOG.info("{} emits {} to {}", irVertex.getId(), output);
    operatorMetricCollector.emittedCnt += 1;

    //LOG.info("Offloading {}, Start offloading {}, End offloading {}, in {}",
    //  offloading, startOffloading, endOffloading, irVertex.getId());

    if (irVertex.isSink) {
      operatorMetricCollector.processDone(inputTimestamp);
    }

    /*
    if (endOffloading) {
      LOG.info("Operator {} end to offload", irVertex.getId());
      offloading = false;
      operatorMetricCollector.endOffloading();
      endOffloading = false;
      currOffloadingContext = null;

      if (startOffloading) {
        // this means that it does not receive any event during offloading
        // we then disable the start offloading
        startOffloading = false;
      }
    }

    if (startOffloading) {
      LOG.info("Operator {} start to offload", irVertex.getId());
      operatorMetricCollector.startOffloading();
      startOffloading = false;
      offloading = true;
    }
    */

    // For offloading
    List<String> offloadingIds = null;

    for (final NextIntraTaskOperatorInfo internalVertex : internalMainOutputs) {
      final OperatorVertex nextOperator = internalVertex.getNextOperator();

      //LOG.info("NexOp: {}, isOffloading: {}, isOffloaded: {}",
      //  nextOperator.getId(), nextOperator.isOffloading, isOffloaded.get());

      if (offloading) {
        if (offloadingIds == null) {
          offloadingIds = new LinkedList<>();
        }
        offloadingIds.add(nextOperator.getId());
      } else {
        final Pair<OperatorMetricCollector, OutputCollector> pair =
          outputCollectorMap.get(nextOperator.getId());
        ((OperatorVertexOutputCollector) pair.right()).inputTimestamp = inputTimestamp;
        emit(nextOperator, output);
      }
    }

    // For offloading
    if (offloadingIds != null) {
      operatorMetricCollector.sendToServerless(
        new TimestampAndValue<>(inputTimestamp, output), offloadingIds, currWatermark);
    }

    for (final OutputWriter externalWriter : externalMainOutputs) {
      emit(externalWriter, new TimestampAndValue<>(inputTimestamp, output));
    }
  }

  @Override
  public <T> void emit(final String dstVertexId, final T output) {
    //LOG.info("{} emits {} to {}", irVertex.getId(), output, dstVertexId);
    operatorMetricCollector.emittedCnt += 1;

    List<String> offloadingIds = null;

    if (internalAdditionalOutputs.containsKey(dstVertexId)) {
      for (final NextIntraTaskOperatorInfo internalVertex : internalAdditionalOutputs.get(dstVertexId)) {
        if (offloading) {
          if (offloadingIds == null) {
            offloadingIds = new LinkedList<>();
          }
          offloadingIds.add(internalVertex.getNextOperator().getId());
        } else {
          final Pair<OperatorMetricCollector, OutputCollector> pair =
            outputCollectorMap.get(internalVertex.getNextOperator().getId());
          ((OperatorVertexOutputCollector) pair.right()).inputTimestamp = inputTimestamp;
          emit(internalVertex.getNextOperator(), (O) output);
        }
      }
    }

    // For offloading
    if (offloadingIds != null) {
      operatorMetricCollector.sendToServerless(
        new TimestampAndValue<>(inputTimestamp, output), offloadingIds, currWatermark);
    }

    if (externalAdditionalOutputs.containsKey(dstVertexId)) {
      for (final OutputWriter externalWriter : externalAdditionalOutputs.get(dstVertexId)) {
        emit(externalWriter, new TimestampAndValue<>(inputTimestamp, (O) output));
      }
    }
  }

  @Override
  public void emitWatermark(final Watermark watermark) {

    if (offloading) {
      prevWatermarkMap.put(watermark.getTimestamp(), currWatermark);
    }

    currWatermark = watermark.getTimestamp();

    if (LOG.isDebugEnabled()) {
      LOG.debug("{} emits watermark {}", irVertex.getId(), watermark);
    }

    List<String> offloadingIds = null;

    // Emit watermarks to internal vertices
    for (final NextIntraTaskOperatorInfo internalVertex : internalMainOutputs) {
      if (offloading) {
        if (offloadingIds == null) {
          offloadingIds = new LinkedList<>();
        }
        offloadingIds.add(internalVertex.getNextOperator().getId());
      } else {
        final Pair<OperatorMetricCollector, OutputCollector> pair =
          outputCollectorMap.get(internalVertex.getNextOperator().getId());
        //LOG.info("Internal Watermark {} emit to {}", watermark, internalVertex.getNextOperator().getId());


        internalVertex.getWatermarkManager().trackAndEmitWatermarks(internalVertex.getEdgeIndex(), watermark);
      }
    }

    for (final List<NextIntraTaskOperatorInfo> internalVertices : internalAdditionalOutputs.values()) {
      for (final NextIntraTaskOperatorInfo internalVertex : internalVertices) {
        if (offloading) {
          if (offloadingIds == null) {
            offloadingIds = new LinkedList<>();
          }
          offloadingIds.add(internalVertex.getNextOperator().getId());
        } else {
          final Pair<OperatorMetricCollector, OutputCollector> pair =
            outputCollectorMap.get(internalVertex.getNextOperator().getId());
          //LOG.info("Internal Watermark {} emit to {}", watermark, internalVertex.getNextOperator().getId());


          internalVertex.getWatermarkManager().trackAndEmitWatermarks(internalVertex.getEdgeIndex(), watermark);
        }
      }
    }

    // For offloading
    if (offloadingIds != null) {

      // add this watermark to the expected map
      final List<String> sinks = currOffloadingContext.getOffloadingSinks(irVertex);
      for (final String sink : sinks) {
        LOG.info("Expected watermark for {}: {}", sink, watermark);
        expectedWatermarkMap.get(sink).left().add(watermark);
      }

      operatorMetricCollector.sendToServerless(watermark, offloadingIds, currWatermark);
    }


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
