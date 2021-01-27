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
package org.apache.nemo.runtime.lambdaexecutor.kafka;

import org.apache.nemo.common.dag.Edge;
import org.apache.nemo.common.ir.AbstractOutputCollector;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.punctuation.TimestampAndValue;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.offloading.common.OffloadingOutputCollector;
import org.apache.nemo.runtime.executor.common.NextIntraTaskOperatorInfo;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingResultTimestampEvent;
import org.apache.nemo.runtime.lambdaexecutor.ThpEvent;
import org.apache.nemo.runtime.lambdaexecutor.datatransfer.PipeOutputWriter;
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
public final class KafkaOperatorVertexOutputCollector<O> extends AbstractOutputCollector<O> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaOperatorVertexOutputCollector.class.getName());

  private static final String BUCKET_NAME = "nemo-serverless";

  private final IRVertex irVertex;
  private final Map<String, NextIntraTaskOperatorInfo> internalMainOutputs;
  private final List<NextIntraTaskOperatorInfo> nextOperators;
  private final Map<String, List<NextIntraTaskOperatorInfo>> internalAdditionalOutputs;
  private final Map<String, List<String>> taskOutgoingEdges;

  private final OffloadingOutputCollector offloadingOutputCollector;
  private final Edge edge;
  private final Map<String, KafkaOperatorVertexOutputCollector> outputCollectorMap;
  private final Map<String, List<PipeOutputWriter>> externalAdditionalOutputs;
  private final List<PipeOutputWriter> externalMainOutputs;

  public String watermarkSourceId;

  private final double samplingRate;
  private final Random random = new Random();

  private final Edge encodingEdge;
  private final String taskId;

  private long prevLogtime;
  private int processedCnt = 0;

  private final boolean isSourceVertex;

  /**
   * Constructor of the output collector.
   * @param irVertex the ir vertex that emits the output
   * @param internalAdditionalOutputs internal additional outputs
   */
  public KafkaOperatorVertexOutputCollector(
    final String taskId,
    final IRVertex irVertex,
    final double samplingRate,
    final Edge edge,
    final List<NextIntraTaskOperatorInfo> nextOperators,
    final Map<String, List<NextIntraTaskOperatorInfo>> internalAdditionalOutputs,
    final OffloadingOutputCollector offloadingOutputCollector,
    final Map<String, KafkaOperatorVertexOutputCollector> outputCollectorMap,
    final Map<String, List<String>> taskOutgoingEdges,
    final Map<String, List<PipeOutputWriter>> externalAdditionalOutputs,
    final List<PipeOutputWriter> externalMainOutputs) {
    this.taskId = taskId;
    this.irVertex = irVertex;
    this.isSourceVertex = irVertex instanceof SourceVertex;
    this.samplingRate = samplingRate;
    this.edge = edge;
    this.internalMainOutputs = new HashMap<>();
    this.nextOperators = nextOperators;
    this.prevLogtime = System.currentTimeMillis();
    for (final NextIntraTaskOperatorInfo info : nextOperators) {
      internalMainOutputs.put(info.getNextOperator().getId(), info);
    }

    LOG.info("Sampling rate of vertex " + irVertex.getId() + ": " + samplingRate);

    LOG.info("Hello world!!");

    this.internalAdditionalOutputs = internalAdditionalOutputs;
    this.offloadingOutputCollector = offloadingOutputCollector;
    this.outputCollectorMap = outputCollectorMap;
    this.taskOutgoingEdges = taskOutgoingEdges;
    this.externalAdditionalOutputs = externalAdditionalOutputs;
    this.externalMainOutputs = externalMainOutputs;
    this.encodingEdge = getEncodingEdge();
  }

  private void emit(final OperatorVertex vertex, final O output) {

    //  LOG.info("{} process event to {}", vertex.getId(), vertex.getTransform());

    try {
      vertex.getTransform().onData(output);
    } catch (final Exception e){
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  private Edge getEncodingEdge() {
    if (externalMainOutputs.size() > 0) {
      return externalMainOutputs.get(0).getEdge();
    } else if (externalAdditionalOutputs.size() > 0) {
      return externalAdditionalOutputs.values().stream()
        .collect(Collectors.toList()).get(0).get(0).getEdge();
    } else {
      return edge;
    }
  }

  @Override
  public void emit(final O output) {
    List<String> nextOpIds = null;


     // LOG.info("Output from {}, isSink: {}: {}", irVertex.getId(), irVertex.isSink, output);

    for (final NextIntraTaskOperatorInfo internalVertex : nextOperators) {
      //LOG.info("Set timestamp {} to {}", inputTimestamp, internalVertex.getNextOperator().getId());
      final KafkaOperatorVertexOutputCollector oc =
        outputCollectorMap.get(internalVertex.getNextOperator().getId());
      oc.inputTimestamp = inputTimestamp;
      emit(internalVertex.getNextOperator(), output);
    }

    for (final PipeOutputWriter outputWriter : externalMainOutputs) {
      //LOG.info("Emit to output vertex at {}, ts: {}, val: {}", irVertex.getId(), inputTimestamp, output);

      outputWriter.write(new TimestampAndValue<>(inputTimestamp, output));
    }

    if (isSourceVertex) {
      processedCnt += 1;

      /*
      final long currTime = System.currentTimeMillis();
      if (currTime - prevLogtime >= 1000) {
        offloadingOutputCollector.emit(
          new ThpEvent(taskId, irVertex.getId(), 1000 * processedCnt / (currTime - prevLogtime)));
        processedCnt = 0;
        prevLogtime = System.currentTimeMillis();
      }
      */
    }

    if (irVertex.isSink) {
      //LOG.info("Sink output at {}, ts: {}, val: {}", irVertex.getId(), inputTimestamp, output);
      if (random.nextDouble() < samplingRate) {
        if (nextOpIds == null) {
          nextOpIds = new LinkedList<>();
        }
        nextOpIds.add(irVertex.getId());
      }

      if (nextOpIds != null) {
        //LOG.info("Emit sampling data at {}", irVertex.getId());
        for (final String nextOpId : nextOpIds){
          //LOG.info("Emit to VM at {}, ts: {}, val: {}", irVertex.getId(), inputTimestamp, output);
          offloadingOutputCollector.emit(new OffloadingResultTimestampEvent(taskId, nextOpId, inputTimestamp, 0));
        }
        /*
        resultCollector.result.add(new NemoTriple<>(
          nextOpIds,
          encodingEdge.getId(),
          new TimestampAndValue(inputTimestamp, output)));
          */
      }
    }

  }

  @Override
  public <T> void emit(final String dstVertexId, final T output) {
    //LOG.info("{} emits {} to {}", irVertex.getId(), output, dstVertexId);

    List<String> nextOpIds = null;

    if (irVertex.isSink) {
      if (random.nextDouble() < samplingRate) {
        if (nextOpIds == null) {
          nextOpIds = new LinkedList<>();
        }
        nextOpIds.add(irVertex.getId());
      }

      if (nextOpIds != null) {
        for (final String nextOpId : nextOpIds){
          offloadingOutputCollector.emit(new OffloadingResultTimestampEvent(taskId, nextOpId, inputTimestamp, 0));
        }
        /*
        resultCollector.result.add(new NemoTriple<>(
          nextOpIds,
          encodingEdge.getId(),
          new TimestampAndValue(inputTimestamp, output)));
          */
      }
    }

    if (internalAdditionalOutputs.containsKey(dstVertexId)) {
      for (final NextIntraTaskOperatorInfo internalVertex : internalAdditionalOutputs.get(dstVertexId)) {
        //System.out.print(internalVertex.getNextOperator().getId() + ", ");
        //LOG.info("{} internal emit {} to {}", irVertex.getId(), output, dstVertexId);
        outputCollectorMap.get(internalVertex.getNextOperator().getId()).inputTimestamp = inputTimestamp;
        emit(internalVertex.getNextOperator(), (O) output);
      }
    }


    if (externalAdditionalOutputs.containsKey(dstVertexId)) {
      for (final PipeOutputWriter externalWriter : externalAdditionalOutputs.get(dstVertexId)) {
        //LOG.info("{} external write {} to {}", irVertex.getId(), output, dstVertexId);
        externalWriter.write(new TimestampAndValue<>(inputTimestamp, (O) output));
      }
    }

    // TODO: handle output writer!!
  }

  @Override
  public void emitWatermark(final Watermark watermark) {
//    if (LOG.isDebugEnabled()) {
//      LOG.debug("{} emits watermark {}", irVertex.getId(), watermark);
//    }

    //System.out.println("Operator " + irVertex.getId() + " emits watermark " + watermark);
    // Emit watermarks to internal vertices
    for (final NextIntraTaskOperatorInfo internalVertex : nextOperators) {
      //System.out.println("Operator " + irVertex.getId() + " emits watermark to " + internalVertex.getNextOperator().getId());
      internalVertex.getNextOperator().getTransform().onWatermark(watermark);
    }

    for (final List<NextIntraTaskOperatorInfo> internalVertices : internalAdditionalOutputs.values()) {
      for (final NextIntraTaskOperatorInfo internalVertex : internalVertices) {
        //System.out.println("Operator " + irVertex.getId() + " emits watermark to " + internalVertex.getNextOperator().getId());
        internalVertex.getNextOperator().getTransform().onWatermark(watermark);
      }
    }

    // Emit watermarks to output writer
    for (final PipeOutputWriter outputWriter : externalMainOutputs) {
      //LOG.info("Emit watermark to output writer at {}", irVertex.getId());
      outputWriter.writeWatermark(watermark);
    }

    for (final List<PipeOutputWriter> externalVertices : externalAdditionalOutputs.values()) {
      for (final PipeOutputWriter externalVertex : externalVertices) {
        //LOG.info("Emit watermark to output writer22 at {}", irVertex.getId());
        externalVertex.writeWatermark(watermark);
      }
    }

  }
}
