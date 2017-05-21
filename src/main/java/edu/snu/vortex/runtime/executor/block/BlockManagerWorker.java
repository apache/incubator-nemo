/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.vortex.runtime.executor.block;

import edu.snu.vortex.client.JobConf;
import edu.snu.vortex.compiler.frontend.beam.BeamElement;
import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.common.message.MessageEnvironment;
import edu.snu.vortex.runtime.common.message.MessageSender;
import edu.snu.vortex.runtime.exception.NodeConnectionException;
import edu.snu.vortex.runtime.exception.UnsupportedBlockStoreException;
import edu.snu.vortex.runtime.executor.PersistentConnectionToMaster;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.join.UnionCoder;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;

/**
 * Executor-side block manager.
 * For now, all its operations are synchronized to guarantee thread safety.
 */
@ThreadSafe
public final class BlockManagerWorker {
  private final String executorId;

  private final LocalStore localStore;

  private final Set<String> idOfBlocksStoredInThisWorker;

  private final MessageEnvironment messageEnvironment;

  private final PersistentConnectionToMaster persistentConnectionToMaster;

  @Inject
  public BlockManagerWorker(@Parameter(JobConf.ExecutorId.class) final String executorId,
                            final LocalStore localStore,
                            final PersistentConnectionToMaster persistentConnectionToMaster,
                            final MessageEnvironment messageEnvironment) {
    this.executorId = executorId;
    this.localStore = localStore;
    this.messageEnvironment = messageEnvironment;
    this.persistentConnectionToMaster = persistentConnectionToMaster;
    this.idOfBlocksStoredInThisWorker = new HashSet<>();
  }

  /**
   * Store block somewhere.
   * Invariant: This should be invoked only once per blockId.
   * @param blockId of the block
   * @param data of the block
   * @param blockStore for storing the block
   */
  public synchronized void putBlock(final String blockId,
                                    final Iterable<Element> data,
                                    final RuntimeAttribute blockStore) {
    if (idOfBlocksStoredInThisWorker.contains(blockId)) {
      throw new RuntimeException("Trying to put an already existing block");
    }
    final BlockStore store = getBlockStore(blockStore);
    store.putBlock(blockId, data);
    idOfBlocksStoredInThisWorker.add(blockId);

    persistentConnectionToMaster.getMessageSender().send(
        ControlMessage.Message.newBuilder()
        .setId(RuntimeIdGenerator.generateMessageId())
        .setType(ControlMessage.MessageType.BlockStateChanged)
        .setBlockStateChangedMsg(
            ControlMessage.BlockStateChangedMsg.newBuilder()
            .setExecutorId(executorId)
            .setBlockId(blockId)
            .setState(ControlMessage.BlockStateFromExecutor.COMMITTED)
            .build())
        .build());
  }

  /**
   * Get the stored block.
   * Unlike putBlock, this can be invoked multiple times per blockId (maybe due to failures).
   * Here, we first check if we have the block here, and then try to fetch the block from a remote worker.
   * @param blockId of the block
   * @param blockStore for the data storage
   * @return the block data
   */
  public synchronized Iterable<Element> getBlock(final String blockId, final RuntimeAttribute blockStore) {
    if (idOfBlocksStoredInThisWorker.contains(blockId)) {
      // Local hit!
      final BlockStore store = getBlockStore(blockStore);
      final Optional<Iterable<Element>> optionalData = store.getBlock(blockId);
      if (optionalData.isPresent()) {
        return optionalData.get();
      } else {
        // TODO #163: Handle Fault Tolerance
        // We should report this exception to the master, instead of shutting down the JVM
        throw new RuntimeException("Something's wrong: worker thinks it has the block, but the store doesn't have it");
      }
    } else {
      // We don't have the block here... let's see if a remote worker has it
      // Ask Master for the location

      final ControlMessage.Message responseFromMaster;
      try {
        responseFromMaster = persistentConnectionToMaster.getMessageSender().<ControlMessage.Message>request(
          ControlMessage.Message.newBuilder()
              .setId(RuntimeIdGenerator.generateMessageId())
              .setType(ControlMessage.MessageType.RequestBlockLocation)
              .setRequestBlockLocationMsg(
                  ControlMessage.RequestBlockLocationMsg.newBuilder()
                      .setExecutorId(executorId)
                      .setBlockId(blockId)
                      .build())
              .build()).get();
      } catch (Exception e) {
        throw new NodeConnectionException(e);
      }

      final ControlMessage.BlockLocationInfoMsg blockLocationInfoMsg = responseFromMaster.getBlockLocationInfoMsg();
      assert (responseFromMaster.getType() == ControlMessage.MessageType.BlockLocationInfo);
      if (blockLocationInfoMsg == null) {
        // TODO #163: Handle Fault Tolerance
        // We should report this exception to the master, instead of shutting down the JVM
        throw new RuntimeException("Block " + blockId + " not found both in the local storage and the remote storage");
      }
      final String remoteWorkerId = blockLocationInfoMsg.getOwnerExecutorId();

      // Request the block to the owner executor.
      final MessageSender<ControlMessage.Message> messageSenderToRemoteExecutor;
      try {
        messageSenderToRemoteExecutor =
            messageEnvironment.<ControlMessage.Message>asyncConnect(
                remoteWorkerId, MessageEnvironment.EXECUTOR_MESSAGE_RECEIVER).get();
      } catch (Exception e) {
        throw new NodeConnectionException(e);
      }

      final ControlMessage.Message responseFromRemoteExecutor;
      try {
        responseFromRemoteExecutor =
            messageSenderToRemoteExecutor.<ControlMessage.Message>request(
                ControlMessage.Message.newBuilder()
                .setId(RuntimeIdGenerator.generateMessageId())
                .setType(ControlMessage.MessageType.RequestBlock)
                .setRequestBlockMsg(
                    ControlMessage.RequestBlockMsg.newBuilder()
                    .setExecutorId(executorId)
                    .setBlockId(blockId)
                    .build())
                .build())
                .get();
      } catch (Exception e) {
        throw new NodeConnectionException(e);
      }

      try {
        messageSenderToRemoteExecutor.close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      final ControlMessage.TransferBlockMsg transferBlockMsg = responseFromRemoteExecutor.getTransferBlockMsg();
      if (transferBlockMsg == null) {
        // TODO #163: Handle Fault Tolerance
        // We should report this exception to the master, instead of shutting down the JVM
        throw new RuntimeException("Failed fetching block " + blockId + "from worker " + remoteWorkerId);
      }

      // TODO #199: Introduce Data Plane
      // TODO #197: Improve Serialization/Deserialization Performance
      final List<Element> deserializedData = new ArrayList<>();
      ArrayList<byte[]> data = SerializationUtils.deserialize(transferBlockMsg.getData().toByteArray());
      data.forEach(bytes -> {
        // TODO #18: Support code/data serialization
        if (transferBlockMsg.getIsUnionValue()) {
          final ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
          List<Coder<?>> elementCodecs = Arrays.asList(SerializableCoder.of(double[].class),
              SerializableCoder.of(double[].class));
          UnionCoder coder = UnionCoder.of(elementCodecs);
          KvCoder kvCoder = KvCoder.of(VarIntCoder.of(), coder);
          try {
            final Element element = new BeamElement(kvCoder.decode(stream, Coder.Context.OUTER));
            deserializedData.add(element);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        } else {
          deserializedData.add(SerializationUtils.deserialize(bytes));
        }
      });
      return deserializedData;
    }
  }

  private BlockStore getBlockStore(final RuntimeAttribute blockStore) {
    switch (blockStore) {
      case Local:
        return localStore;
      case Memory:
        // TODO #181: Implement MemoryBlockStore
        return localStore;
      case File:
        // TODO #69: Implement file channel in Runtime
        return localStore;
      case MemoryFile:
        // TODO #69: Implement file channel in Runtime
        return localStore;
      case DistributedStorage:
        // TODO #180: Implement DistributedStorageStore
        return localStore;
      default:
        throw new UnsupportedBlockStoreException(new Exception(blockStore + " is not supported."));
    }
  }
}
