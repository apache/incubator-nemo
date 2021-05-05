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
package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.punctuation.TimestampAndValue;
import org.apache.nemo.common.ir.AbstractOutputCollector;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.executor.common.datatransfer.OutputWriter;
import org.apache.nemo.runtime.message.MessageEnvironment;
import org.apache.nemo.runtime.message.PersistentConnectionToMasterMap;
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
  private final NextIntraTaskOperatorInfo[] internalMainOutputs;
  private final Map<String, List<NextIntraTaskOperatorInfo>> internalAdditionalOutputs;
  private final OutputWriter[] externalMainOutputs;
  private final Map<String, List<OutputWriter>> externalAdditionalOutputs;

  // for logging
  private long inputTimestamp;
  private final OperatorMetricCollector operatorMetricCollector;
  private final String taskId;
  private final double samplingRate;
  private final String executorId;
  private final long latencyLimit;
  private final PersistentConnectionToMasterMap persistentConnectionToMasterMap;
  private final long st = System.currentTimeMillis();
  private final boolean isSink;

  private final MainDataEmitter mainDataEmitter;

  /**
   * Constructor of the output collector.
   * @param irVertex the ir vertex that emits the output
   * @param internalMainOutputs internal main outputs
   * @param internalAdditionalOutputs internal additional outputs
   * @param externalMainOutputs external main outputs
   * @param externalAdditionalOutputs external additional outputs
   */
  public OperatorVertexOutputCollector(
    final String executorId,
    final Map<String, Pair<OperatorMetricCollector, OutputCollector>> outputCollectorMap,
    final IRVertex irVertex,
    final List<NextIntraTaskOperatorInfo> internalMainOutputs,
    final Map<String, List<NextIntraTaskOperatorInfo>> internalAdditionalOutputs,
    final List<OutputWriter> externalMainOutputs,
    final Map<String, List<OutputWriter>> externalAdditionalOutputs,
    final OperatorMetricCollector operatorMetricCollector,
    final String taskId,
    final Map<String, Double> samplingMap,
    final Long latencyLimit,
    final PersistentConnectionToMasterMap persistentConnectionToMasterMap) {
    this.executorId = executorId;
    this.outputCollectorMap = outputCollectorMap;
    this.irVertex = irVertex;
    this.taskId = taskId;
    this.internalMainOutputs = new NextIntraTaskOperatorInfo[internalMainOutputs.size()];
    internalMainOutputs.toArray(this.internalMainOutputs);
    this.internalAdditionalOutputs = internalAdditionalOutputs;
    this.externalMainOutputs = new OutputWriter[externalMainOutputs.size()];
    externalMainOutputs.toArray(this.externalMainOutputs);
    this.externalAdditionalOutputs = externalAdditionalOutputs;
    this.operatorMetricCollector = operatorMetricCollector;
    this.samplingRate = samplingMap.getOrDefault(irVertex.getId(), 0.0);
    this.latencyLimit = latencyLimit;
    this.persistentConnectionToMasterMap = persistentConnectionToMasterMap;
    this.isSink = this.internalMainOutputs.length == 0 && this.externalMainOutputs.length == 0
      && internalAdditionalOutputs.size() == 0 && externalAdditionalOutputs.size() == 0;

    LOG.info("Vertex {} is sink {}", irVertex.getId(), isSink);
    this.mainDataEmitter = getMainDataEmitter();
    // LOG.info("Vertex Id Sampling Rate {} / {} / {}", irVertex.getId(), samplingRate, samplingMap);
  }

  private MainDataEmitter getMainDataEmitter() {
    if (isSink) {
      return new SinkEmtter();
    }

    if (internalMainOutputs.length > 0 && externalMainOutputs.length > 0) {
      return new InternalExternalMainEmitter();
    } else if (internalMainOutputs.length > 0) {
      return new InternalMainDataEmitter();
    } else {
      return new ExternalMainEmitter();
    }
  }

  public IRVertex getIRVertex() {
    return irVertex;
  }

  private void emit(final OperatorVertex vertex, final Object output) {
    vertex.getTransform().onData(output);
  }

  private void emit(final OutputWriter writer, final TimestampAndValue output) {
    // metric collection
    //LOG.info("Write {} to writer {}", output, writer);
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

    //LOG.info("Offloading {}, Start prepareOffloading {}, End prepareOffloading {}, in {}",
    //  prepareOffloading, startOffloading, endOffloading, irVertex.getId());

    mainDataEmitter.emitData(output);

    /*
    // For sampling
    final long currTime = System.currentTimeMillis();
    final int latency = (int) ((currTime - inputTimestamp));

    if (samplingRate > 0 && currTime - prevLogTime >= 500) {
      prevLogTime = currTime;
      LOG.info("Event Latency {} from {} in {}/{} ", latency,
        irVertex.getId(),
        executorId,
        taskId,
        output);

      if (latency > latencyLimit) {
        persistentConnectionToMasterMap
          .getMessageSender(MessageEnvironment.ListenerType.RUNTIME_MASTER_MESSAGE_LISTENER_ID)
          .send(ControlMessage.Message.newBuilder()
            .setId(RuntimeIdManager.generateMessageId())
            .setListenerId(MessageEnvironment.ListenerType.RUNTIME_MASTER_MESSAGE_LISTENER_ID.ordinal())
            .setType(ControlMessage.MessageType.LatencyCollection)
            .setLatencyMsg(ControlMessage.LatencyCollectionMessage.newBuilder()
              .setExecutorId(executorId)
              .setLatency(latency)
              .build())
            .build());
      }
    }
      */



    /*
    if (random.nextDouble() <= samplingRate) {
      LOG.info("Event Latency {} from {} in {}/{} ", latency,
        irVertex.getId(),
        executorId,
        taskId,
        output);
        */
    // }
  }

  @Override
  public <T> void emit(final String dstVertexId, final T output) {
    //LOG.info("{} emits {} to {}", irVertex.getId(), output, dstVertexId);
    if (internalAdditionalOutputs.containsKey(dstVertexId)) {
      for (final NextIntraTaskOperatorInfo internalVertex : internalAdditionalOutputs.get(dstVertexId)) {
        final Pair<OperatorMetricCollector, OutputCollector> pair =
          outputCollectorMap.get(internalVertex.getNextOperator().getId());
        pair.right().setInputTimestamp(inputTimestamp);
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
    //if (LOG.isDebugEnabled()) {
       // LOG.info("{}/{} emits watermark {}", irVertex.getId(), taskId, watermark.getTimestamp());
    //}

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

        internalVertex.getNextOperator().getTransform().onWatermark(watermark);
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
          internalVertex.getNextOperator().getTransform().onWatermark(watermark);
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

  interface MainDataEmitter {
    void emitData(Object data);
  }


  final class SinkEmtter implements MainDataEmitter {
    @Override
    public void emitData(Object data) {
      operatorMetricCollector.processDone(inputTimestamp,
        irVertex.getId(), executorId, taskId, latencyLimit, data, persistentConnectionToMasterMap);
    }
  }

  final class InternalMainDataEmitter implements MainDataEmitter {

    @Override
    public void emitData(Object data) {
      for (final NextIntraTaskOperatorInfo internalVertex : internalMainOutputs) {
        final OperatorVertex nextOperator = internalVertex.getNextOperator();

        //LOG.info("NexOp: {}, isOffloading: {}, isOffloadedTask: {}",
        //  nextOperator.getId(), nextOperator.isOffloading, isOffloadedTask.get());

        final Pair<OperatorMetricCollector, OutputCollector> pair =
          outputCollectorMap.get(nextOperator.getId());
        pair.right().setInputTimestamp(inputTimestamp);
        emit(nextOperator, data);
      }
    }
  }

  final class ExternalMainEmitter implements MainDataEmitter {

    @Override
    public void emitData(Object data) {
      for (final OutputWriter externalWriter : externalMainOutputs) {
        emit(externalWriter, new TimestampAndValue<>(inputTimestamp, data));
      }
    }
  }

  final class InternalExternalMainEmitter implements MainDataEmitter {

    @Override
    public void emitData(Object data) {
       for (final NextIntraTaskOperatorInfo internalVertex : internalMainOutputs) {
        final OperatorVertex nextOperator = internalVertex.getNextOperator();

        //LOG.info("NexOp: {}, isOffloading: {}, isOffloadedTask: {}",
        //  nextOperator.getId(), nextOperator.isOffloading, isOffloadedTask.get());

        final Pair<OperatorMetricCollector, OutputCollector> pair =
          outputCollectorMap.get(nextOperator.getId());
        pair.right().setInputTimestamp(inputTimestamp);
        emit(nextOperator, data);
      }

      for (final OutputWriter externalWriter : externalMainOutputs) {
        emit(externalWriter, new TimestampAndValue<>(inputTimestamp, data));
      }
    }
  }


  interface WatermarkEmitter {
    void emitWatermark(final Watermark watermark);
  }
}
