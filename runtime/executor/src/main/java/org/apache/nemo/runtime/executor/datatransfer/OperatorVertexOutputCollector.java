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
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.runtime.executor.common.NextIntraTaskOperatorInfo;
import org.apache.nemo.common.punctuation.TimestampAndValue;
import org.apache.nemo.common.ir.AbstractOutputCollector;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.runtime.executor.common.TaskExecutor;
import org.apache.nemo.runtime.executor.task.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

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
  private final TaskExecutor taskExecutor;

  private final String edgeId;

  private final boolean isSourceVertex;

  private final String taskId;
  private final double samplingRate;

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
    final Map<String, Pair<PriorityQueue<Watermark>, PriorityQueue<Watermark>>> expectedWatermarkMap,
    final TaskExecutor taskExecutor,
    final String edgeId,
    final String taskId,
    final Map<String, Double> samplingMap) {
    this.outputCollectorMap = outputCollectorMap;
    this.irVertex = irVertex;
    this.taskId = taskId;
    this.internalMainOutputs = internalMainOutputs;
    this.internalAdditionalOutputs = internalAdditionalOutputs;
    this.externalMainOutputs = externalMainOutputs;
    this.externalAdditionalOutputs = externalAdditionalOutputs;
    this.operatorMetricCollector = operatorMetricCollector;
    this.prevWatermarkMap = prevWatermarkMap;
    this.expectedWatermarkMap = expectedWatermarkMap;
    this.taskExecutor = taskExecutor;
    this.edgeId = edgeId;
    this.samplingRate = samplingMap.getOrDefault(irVertex.getId(), 1.0);
    this.isSourceVertex = irVertex instanceof SourceVertex;
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

  @Override
  public void emit(final O output) {
    //LOG.info("{} emits {} to {}", irVertex.getId(), output);

    //LOG.info("Offloading {}, Start offloading {}, End offloading {}, in {}",
    //  offloading, startOffloading, endOffloading, irVertex.getId());

    if (irVertex.isSink) {
      operatorMetricCollector.processDone(inputTimestamp);
    }

    // offloading
    /* TODO: for middleOffloader
    if (taskExecutor.isOffloaded() && !offloadingMainIds.isEmpty()) {
      //LOG.info("Offloading data to serverless: {} at {}", output, taskExecutor.getId());
      taskExecutor.sendToServerless(
        new TimestampAndValue<>(inputTimestamp, output), offloadingMainIds, currWatermark, edgeId);
      return;
    }
    */

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

      final Pair<OperatorMetricCollector, OutputCollector> pair =
        outputCollectorMap.get(nextOperator.getId());
      ((OperatorVertexOutputCollector) pair.right()).inputTimestamp = inputTimestamp;
      emit(nextOperator, output);
    }

    for (final OutputWriter externalWriter : externalMainOutputs) {
      emit(externalWriter, new TimestampAndValue<>(inputTimestamp, output));
    }


    // calculate thp
    if (isSourceVertex) {
      proceseedCnt += 1;
      final long currTime = System.currentTimeMillis();

      /*
      if (currTime - prevLogTime >= 1000) {
        LOG.info("Thp: {} at {}", (1000* proceseedCnt / (currTime - prevLogTime)),
          taskId);
        proceseedCnt = 0;
        prevLogTime = currTime;
      }
      */

      if (random.nextDouble() < samplingRate) {
        final int latency = (int)((currTime - inputTimestamp));
        LOG.info("Event Latency {} from {} in {}", latency,
          irVertex.getId(),
          taskId);
      }
    }
  }

  int proceseedCnt = 0;
  long prevLogTime = System.currentTimeMillis();
  private final Random random = new Random();

  @Override
  public <T> void emit(final String dstVertexId, final T output) {
    //LOG.info("{} emits {} to {}", irVertex.getId(), output, dstVertexId);

    if (internalAdditionalOutputs.containsKey(dstVertexId)) {
      for (final NextIntraTaskOperatorInfo internalVertex : internalAdditionalOutputs.get(dstVertexId)) {
        final Pair<OperatorMetricCollector, OutputCollector> pair =
          outputCollectorMap.get(internalVertex.getNextOperator().getId());
        ((OperatorVertexOutputCollector) pair.right()).inputTimestamp = inputTimestamp;
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


    // offloading
    /* TODO: for middle offloader
    if (taskExecutor.isOffloaded() && !offloadingIds.isEmpty()) {
      LOG.info("Offloading watermark to serverless: {} at {}", watermark, taskExecutor.getId());
      taskExecutor.sendToServerless(
        watermark, offloadingIds, currWatermark, edgeId);
      return;
    }
    */

    //LOG.info("Emit watermark {} from {}", watermark, irVertex.getId());
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
