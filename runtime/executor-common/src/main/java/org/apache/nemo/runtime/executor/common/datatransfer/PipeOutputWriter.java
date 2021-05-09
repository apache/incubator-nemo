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
package org.apache.nemo.runtime.executor.common.datatransfer;

import io.netty.buffer.ByteBuf;
import org.apache.nemo.common.TaskMetrics;
import org.apache.nemo.common.exception.UnsupportedCommPatternException;
import org.apache.nemo.common.punctuation.TimestampAndValue;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.punctuation.WatermarkWithIndex;
import org.apache.nemo.runtime.executor.common.Serializer;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.partitioner.Partitioner;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Represents the output data transfer from a task.
 */
public final class PipeOutputWriter implements OutputWriter {
  private static final Logger LOG = LoggerFactory.getLogger(OutputWriter.class.getName());

  private final String srcTaskId;
  private final int srcTaskIndex;
  private final PipeManagerWorker pipeManagerWorker;

  private final Partitioner partitioner;
  private final RuntimeEdge runtimeEdge;

  private boolean initialized;
  private final Serializer serializer;
  private final List<String> dstTaskIds;
  private final StageEdge stageEdge;

  private volatile boolean stopped = false;


  private final String stageId;
  private final TaskMetrics taskMetrics;

  /**
   * Constructor.
   *
   * @param srcTaskId           the id of the source task.
   * @param runtimeEdge         the {@link RuntimeEdge}.
   * @param pipeManagerWorker   the pipe manager.
   */
  public PipeOutputWriter(final String srcTaskId,
                   final RuntimeEdge runtimeEdge,
                   final PipeManagerWorker pipeManagerWorker,
                   final Serializer serializer,
                   final TaskMetrics taskMetrics) {
    this.stageEdge = (StageEdge) runtimeEdge;
    this.initialized = false;
    this.srcTaskId = srcTaskId;
    this.stageId = RuntimeIdManager.getStageIdFromTaskId(srcTaskId);
    this.taskMetrics = taskMetrics;
    this.pipeManagerWorker = pipeManagerWorker;
    //this.pipeManagerWorker.notifyMaster(runtimeEdge.getId(), RuntimeIdManager.getIndexFromTaskId(srcTaskId));
    this.partitioner = Partitioner
      .getPartitioner(stageEdge.getExecutionProperties(), stageEdge.getDstIRVertex().getExecutionProperties());
    LOG.info("Partitioner for {}: {}", srcTaskId, partitioner);
    this.runtimeEdge = runtimeEdge;
    this.srcTaskIndex = RuntimeIdManager.getIndexFromTaskId(srcTaskId);
    // this.serializer = serializerManager.getSerializer(runtimeEdge.getId());
    this.serializer = serializer;
    this.dstTaskIds = doInitialize();
  }

  private void writeData(final Object element,
                         final List<String> dstList, final boolean flush) {
    dstList.forEach(dstTask -> {
      pipeManagerWorker.writeData(srcTaskId, runtimeEdge.getId(), dstTask, serializer, element);
    });
  }

  @Override
  public String toString() {
    return runtimeEdge.toString();
  }

  /**
   * Writes output element.
   * This method is not a thread-safe.
   * @param element the element to write.
   */
  @Override
  public void write(final Object element) {

//    if (srcTaskId.contains("Stage2") || srcTaskId.contains("Stage8")) {
//      LOG.info("Writing element from {}: {}", srcTaskId,
//        ((TimestampAndValue)element).value);
//    }

    taskMetrics.incrementOutputElement();
    //executorMetrics.increaseOutputCounter(stageId);

    writeData(element, getPipeToWrite(element), false);
  }

  @Override
  public void write(Object element, String dstTaskId) {
    pipeManagerWorker.writeData(srcTaskId, runtimeEdge.getId(), dstTaskId, serializer, element);
  }

  @Override
  public void writeByteBuf(final ByteBuf element) {
    getPipeToWrite(element).forEach(dstTask -> {
      pipeManagerWorker.writeByteBufData(srcTaskId, runtimeEdge.getId(), dstTask, element);
    });
  }

  @Override
  public void writeByteBuf(ByteBuf byteBuf, String dstTaskId) {
    pipeManagerWorker.writeByteBufData(srcTaskId, runtimeEdge.getId(), dstTaskId, byteBuf);
  }

  @Override
  public void writeWatermark(final Watermark watermark) {


    taskMetrics.setOutputWatermark(watermark.getTimestamp());
    taskMetrics.incrementOutWatermarkCount();

//    if (srcTaskId.contains("Stage2") || srcTaskId.contains("Stage3")) {
//      LOG.info("Output watermark of {}: {} to {} / {}", srcTaskId,
//        new Instant(watermark.getTimestamp()),
//        Thread.currentThread());
//    }

    dstTaskIds.forEach(dstTaskId-> {
      pipeManagerWorker.writeData(srcTaskId, runtimeEdge.getId(), dstTaskId, serializer,
        new WatermarkWithIndex(watermark, srcTaskIndex));
    });

    /*
    pipeManagerWorker.broadcast(srcTaskId,
      runtimeEdge.getId(),
      dstTaskIds, serializer, new WatermarkWithIndex(watermark, srcTaskIndex));
      */

    // offloadIntermediateData(new WatermarkWithIndex(watermark, srcTaskIndex), pipes, false);

    // 여기서 마스터에게 보내면됨.
    // rendevousServerClient.sendWatermark(srcTaskId, watermark.getTimestamp());
  }

  @Override
  public Optional<Long> getWrittenBytes() {
    return Optional.empty();
  }

  @Override
  public void close() {
  }

  @Override
  public Future<Boolean> stop(final String taskId) {
    // send stop message!
    stopped = true;
    throw new RuntimeException("Stop exception " + taskId);
  }

  @Override
  public void restart(final String taskId) {
    stopped = false;
  }

  private List<String> doInitialize() {
    LOG.info("Start - doInitialize() {}", runtimeEdge);
    initialized = true;

    /**********************************************************/

    final Optional<CommunicationPatternProperty.Value> comValue =
      runtimeEdge.getPropertyValue(CommunicationPatternProperty.class);

    final List<String> dstTaskIds;
    if (comValue.get().equals(CommunicationPatternProperty.Value.OneToOne)
      || comValue.get().equals(CommunicationPatternProperty.Value.TransientOneToOne)) {
      dstTaskIds = Collections.singletonList(
        RuntimeIdManager.generateTaskId(stageEdge.getDst().getId(),srcTaskIndex, 0));
      LOG.info("Writing data: edge: {}, Task {}, Dest {}", runtimeEdge.getId(), srcTaskId, srcTaskIndex);
    } else if (comValue.get().equals(CommunicationPatternProperty.Value.BroadCast)
      || comValue.get().equals(CommunicationPatternProperty.Value.TransientBroadcast)
      || comValue.get().equals(CommunicationPatternProperty.Value.Shuffle)
      || comValue.get().equals(CommunicationPatternProperty.Value.TransientShuffle)
      || comValue.get().equals(CommunicationPatternProperty.Value.TransientRR)
      || comValue.get().equals(CommunicationPatternProperty.Value.RoundRobin)) {

      final List<Integer> dstIndices = stageEdge.getDst().getTaskIndices();
      dstTaskIds =
        dstIndices.stream()
          .map(dstTaskIndex ->
            RuntimeIdManager.generateTaskId(stageEdge.getDst().getId(), dstTaskIndex, 0))
          .collect(Collectors.toList());
      LOG.info("Writing data: edge: {}, Task {}, Dest {}", runtimeEdge.getId(), srcTaskId, dstIndices);
    } else {
      throw new UnsupportedCommPatternException(new Exception("Communication pattern not supported " + comValue));
    }
    return dstTaskIds;
  }

  final Random random = new Random();

  private List<String> getPipeToWrite(final Object value) {
    final CommunicationPatternProperty.Value comm =
      (CommunicationPatternProperty.Value) runtimeEdge.getPropertyValue(CommunicationPatternProperty.class).get();
    if (comm.equals(CommunicationPatternProperty.Value.OneToOne)
      || comm.equals(CommunicationPatternProperty.Value.TransientOneToOne)) {
      return Collections.singletonList(dstTaskIds.get(0));
    } else if (comm.equals(CommunicationPatternProperty.Value.BroadCast)
      || comm.equals(CommunicationPatternProperty.Value.TransientBroadcast)) {
      return dstTaskIds;
    } else if (comm.equals(CommunicationPatternProperty.Value.RoundRobin) ||
      comm.equals(CommunicationPatternProperty.Value.TransientRR) ) {
      // RoundRobin
      return Collections.singletonList(dstTaskIds.get(random.nextInt(dstTaskIds.size())));
    } else {
      // Shuffle
      if (value instanceof TimestampAndValue) {
        final int partitionKey = (int) partitioner.partition(((TimestampAndValue)value).value);
        final String dstTask = dstTaskIds.get(partitionKey);
//        if (srcTaskId.contains("Stage2-") || srcTaskId.contains("Stage8")) {
//          LOG.info("Partition key {} from {} to {} for {}", partitionKey,
//            srcTaskId, dstTask, runtimeEdge.getId());
//        }
        return Collections.singletonList(dstTask);
      } else {
        final int partitionKey = (int) partitioner.partition(value);
        final String dstTask = dstTaskIds.get(partitionKey);
//        if (srcTaskId.contains("Stage2-") || srcTaskId.contains("Stage8")) {
//          LOG.info("Partition key {} from {} to {} for {}", partitionKey,
//            srcTaskId, dstTask, runtimeEdge.getId());
//        }
        return Collections.singletonList(dstTask);
      }
    }
  }
}
