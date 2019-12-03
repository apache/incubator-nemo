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
package org.apache.nemo.runtime.executor.data;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.runtime.common.plan.RuntimeEdge;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.runtime.executor.bytetransfer.ByteInputContext;
import org.apache.nemo.runtime.executor.bytetransfer.ByteOutputContext;
import org.apache.nemo.runtime.executor.bytetransfer.ByteTransfer;
import org.apache.nemo.runtime.executor.data.streamchainer.Serializer;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Two threads use this class
 * - Network thread: Saves pipe connections created from destination tasks.
 * - Task executor thread: Creates new pipe connections to destination tasks (read),
 * or retrieves a saved pipe connection (write)
 */
@ThreadSafe
public final class PipeManagerWorker {
  private static final Logger LOG = LoggerFactory.getLogger(PipeManagerWorker.class.getName());

  private final String executorId;
  private final SerializerManager serializerManager;

  // To-Executor connections
  private final ByteTransfer byteTransfer;

  // Thread-safe container
  private final PipeContainer pipeContainer;

  private final PersistentConnectionToMasterMap toMaster;

  @Inject
  private PipeManagerWorker(@Parameter(JobConf.ExecutorId.class) final String executorId,
                            final ByteTransfer byteTransfer,
                            final SerializerManager serializerManager,
                            final PersistentConnectionToMasterMap toMaster) {
    this.executorId = executorId;
    this.byteTransfer = byteTransfer;
    this.serializerManager = serializerManager;
    this.pipeContainer = new PipeContainer();
    this.toMaster = toMaster;
  }

  public CompletableFuture<DataUtil.IteratorWithNumBytes> read(final int srcTaskIndex,
                                                               final RuntimeEdge runtimeEdge,
                                                               final int dstTaskIndex) {
    final String runtimeEdgeId = runtimeEdge.getId();
    // Get the location of the src task (blocking call)
    final CompletableFuture<ControlMessage.Message> responseFromMasterFuture = toMaster
      .getMessageSender(MessageEnvironment.PIPE_MANAGER_MASTER_MESSAGE_LISTENER_ID).request(
        ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(MessageEnvironment.PIPE_MANAGER_MASTER_MESSAGE_LISTENER_ID)
          .setType(ControlMessage.MessageType.RequestPipeLoc)
          .setRequestPipeLocMsg(
            ControlMessage.RequestPipeLocationMessage.newBuilder()
              .setExecutorId(executorId)
              .setRuntimeEdgeId(runtimeEdgeId)
              .setSrcTaskIndex(srcTaskIndex)
              .build())
          .build());


    return responseFromMasterFuture.thenCompose(responseFromMaster -> {
      // Get executor id
      if (responseFromMaster.getType() != ControlMessage.MessageType.PipeLocInfo) {
        throw new RuntimeException("Response message type mismatch!");
      }
      final ControlMessage.PipeLocationInfoMessage pipeLocInfo = responseFromMaster.getPipeLocInfoMsg();
      if (!pipeLocInfo.hasExecutorId()) {
        throw new IllegalStateException();
      }
      final String targetExecutorId = responseFromMaster.getPipeLocInfoMsg().getExecutorId();

      // Descriptor
      final ControlMessage.PipeTransferContextDescriptor descriptor =
        ControlMessage.PipeTransferContextDescriptor.newBuilder()
          .setRuntimeEdgeId(runtimeEdgeId)
          .setSrcTaskIndex(srcTaskIndex)
          .setDstTaskIndex(dstTaskIndex)
          .setNumPipeToWait(getNumOfPipeToWait(runtimeEdge))
          .build();

      // Connect to the executor
      return byteTransfer.newInputContext(targetExecutorId, descriptor.toByteArray(), true)
        .thenApply(context -> new DataUtil.InputStreamIterator(context.getInputStreams(),
          serializerManager.getSerializer(runtimeEdgeId)));
    });
  }


  public void notifyMaster(final String runtimeEdgeId, final long srcTaskIndex) {
    // Notify the master that we're using this pipe.
    toMaster.getMessageSender(MessageEnvironment.PIPE_MANAGER_MASTER_MESSAGE_LISTENER_ID).send(
      ControlMessage.Message.newBuilder()
        .setId(RuntimeIdManager.generateMessageId())
        .setListenerId(MessageEnvironment.PIPE_MANAGER_MASTER_MESSAGE_LISTENER_ID)
        .setType(ControlMessage.MessageType.PipeInit)
        .setPipeInitMsg(ControlMessage.PipeInitMessage.newBuilder()
          .setRuntimeEdgeId(runtimeEdgeId)
          .setSrcTaskIndex(srcTaskIndex)
          .setExecutorId(executorId)
          .build())
        .build());
  }

  /**
   * (SYNCHRONIZATION) Called by task threads.
   *
   * @param runtimeEdge  runtime edge
   * @param srcTaskIndex source task index
   * @return output contexts.
   */
  public List<ByteOutputContext> getOutputContexts(final RuntimeEdge runtimeEdge,
                                                   final long srcTaskIndex) {

    // First, initialize the pair key
    final Pair<String, Long> pairKey = Pair.of(runtimeEdge.getId(), srcTaskIndex);
    pipeContainer.putPipeListIfAbsent(pairKey, getNumOfPipeToWait(runtimeEdge));

    // Then, do stuff
    return pipeContainer.getPipes(pairKey); // blocking call
  }

  public Serializer getSerializer(final String runtimeEdgeId) {
    return serializerManager.getSerializer(runtimeEdgeId);
  }

  /**
   * (SYNCHRONIZATION) Called by network threads.
   *
   * @param outputContext output context
   * @throws InvalidProtocolBufferException protobuf exception
   */
  public void onOutputContext(final ByteOutputContext outputContext) throws InvalidProtocolBufferException {
    final ControlMessage.PipeTransferContextDescriptor descriptor =
      ControlMessage.PipeTransferContextDescriptor.PARSER.parseFrom(outputContext.getContextDescriptor());

    final long srcTaskIndex = descriptor.getSrcTaskIndex();
    final String runtimeEdgeId = descriptor.getRuntimeEdgeId();
    final int dstTaskIndex = (int) descriptor.getDstTaskIndex();
    final int numPipeToWait = (int) descriptor.getNumPipeToWait();
    final Pair<String, Long> pairKey = Pair.of(runtimeEdgeId, srcTaskIndex);

    // First, initialize the pair key
    pipeContainer.putPipeListIfAbsent(pairKey, numPipeToWait);

    // Then, do stuff
    pipeContainer.putPipe(pairKey, dstTaskIndex, outputContext);
  }

  public void onInputContext(final ByteInputContext inputContext) throws InvalidProtocolBufferException {
    throw new UnsupportedOperationException();
  }

  private int getNumOfPipeToWait(final RuntimeEdge runtimeEdge) {
    final int dstParallelism = ((StageEdge) runtimeEdge).getDstIRVertex().getPropertyValue(ParallelismProperty.class)
      .orElseThrow(() -> new IllegalStateException());
    final CommunicationPatternProperty.Value commPattern = ((StageEdge) runtimeEdge)
      .getPropertyValue(CommunicationPatternProperty.class)
      .orElseThrow(() -> new IllegalStateException());

    return commPattern.equals(CommunicationPatternProperty.Value.ONE_TO_ONE) ? 1 : dstParallelism;
  }
}
