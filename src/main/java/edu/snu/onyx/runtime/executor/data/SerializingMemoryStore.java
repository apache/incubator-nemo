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
package edu.snu.onyx.runtime.executor.data;

import edu.snu.onyx.common.coder.Coder;
import edu.snu.onyx.compiler.ir.Element;
import edu.snu.onyx.runtime.common.RuntimeIdGenerator;
import edu.snu.onyx.runtime.exception.PartitionFetchException;
import edu.snu.onyx.runtime.exception.PartitionWriteException;
import edu.snu.onyx.runtime.executor.data.partition.SerializedMemoryPartition;
import org.apache.reef.tang.InjectionFuture;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Serialize and store data in local memory.
 */
@ThreadSafe
public final class SerializingMemoryStore implements PartitionStore {
  public static final String SIMPLE_NAME = "SerializingMemoryStore";
  // A map between partition id and data blocks.
  private final ConcurrentHashMap<String, SerializedMemoryPartition> partitionMap;
  private final InjectionFuture<PartitionManagerWorker> partitionManagerWorker;

  @Inject
  private SerializingMemoryStore(final InjectionFuture<PartitionManagerWorker> partitionManagerWorker) {
    this.partitionMap = new ConcurrentHashMap<>();
    this.partitionManagerWorker = partitionManagerWorker;
  }

  /**
   * @see PartitionStore#getFromPartition(String, HashRange).
   */
  @Override
  public Optional<Iterable<Element>> getFromPartition(final String partitionId,
                                                      final HashRange hashRange) {
    final SerializedMemoryPartition partition = partitionMap.get(partitionId);

    try {
      if (partition != null) {
        final Coder coder = getCoderFromWorker(partitionId);
        final Iterable<SerializedMemoryPartition.SerializedBlock> blocks = partition.getBlocks();
        final List<Element> deserializedData = new ArrayList<>();
        for (final SerializedMemoryPartition.SerializedBlock serializedBlock : blocks) {
          final int hashVal = serializedBlock.getKey();
          if (hashRange.includes(hashVal)) {
            // The hash value of this block is in the range.
            final long numElements = serializedBlock.getElementsInBlock();
            // This stream will be not closed, but it is okay as long as the file stream is closed well.
            final ByteArrayInputStream byteArrayInputStream =
                new ByteArrayInputStream(serializedBlock.getSerializedData());
            for (int i = 0; i < numElements; i++) {
              deserializedData.add(coder.decode(byteArrayInputStream));
            }
          }
        }

        return Optional.of(deserializedData);
      } else {
        return Optional.empty();
      }
    } catch (final IOException e) {
      throw new PartitionFetchException(e);
    }
  }

  /**
   * Retrieves serialized data (array of bytes) in a specific {@link HashRange} from a partition.
   *
   * @param partitionId of the target partition.
   * @param hashRange   the hash range.
   * @return the result data from the target partition (if the target partition exists).
   */
  public Optional<Iterable<byte[]>> getSerializedBlocksFromPartition(final String partitionId,
                                                                     final HashRange hashRange) {
    final SerializedMemoryPartition partition = partitionMap.get(partitionId);

    if (partition != null) {
      final Iterable<SerializedMemoryPartition.SerializedBlock> blocks = partition.getBlocks();
      final List<byte[]> deserializedData = new ArrayList<>();
      blocks.forEach(serializedBlock -> {
        final int hashVal = serializedBlock.getKey();
        if (hashRange.includes(hashVal)) {
          // The hash value of this block is in the range.
          deserializedData.add(serializedBlock.getSerializedData());
        }
      });

      return Optional.of(deserializedData);
    } else {
      return Optional.empty();
    }
  }

  /**
   * @see PartitionStore#putToPartition(String, Iterable, boolean).
   */
  @Override
  public Optional<List<Long>> putToPartition(final String partitionId,
                                             final Iterable<Block> blocks,
                                             final boolean commitPerBlock) throws PartitionWriteException {
    partitionMap.putIfAbsent(partitionId, new SerializedMemoryPartition());
    try {
      final Coder coder = getCoderFromWorker(partitionId);
      final List<Long> blockSizeList = new ArrayList<>();
      final SerializedMemoryPartition partition = partitionMap.get(partitionId);
      // Serialize the given blocks
      final ByteArrayOutputStream bytesOutputStream = new ByteArrayOutputStream();
      for (final Block block : blocks) {
        long numOfElementsInBlock = 0;
        for (final Element element : block.getData()) {
          coder.encode(element, bytesOutputStream);
          numOfElementsInBlock++;
        }

        // Write the block.
        final long blockSize = writeBlock(numOfElementsInBlock, bytesOutputStream, partition, block.getKey());
        blockSizeList.add(blockSize);
        bytesOutputStream.reset();
      }

      return Optional.of(blockSizeList);
    } catch (final IOException e) {
      // The partition is committed already.
      throw new PartitionWriteException(new Throwable("This partition is already committed."));
    }
  }

  /**
   * @see PartitionStore#commitPartition(String).
   */
  @Override
  public void commitPartition(final String partitionId) {
    final SerializedMemoryPartition partition = partitionMap.get(partitionId);
    if (partition != null) {
      partition.commit();
    } else {
      throw new PartitionWriteException(new Throwable("There isn't any partition with id " + partitionId));
    }
  }

  /**
   * @see PartitionStore#removePartition(String).
   */
  @Override
  public Boolean removePartition(final String partitionId) {
    return partitionMap.remove(partitionId) != null;
  }

  /**
   * Makes the given stream to a block and write it to the given file partition.
   *
   * @param elementsInBlock the number of elements in this block.
   * @param outputStream    the output stream containing data.
   * @param partition       the partition to write the block.
   * @param hashVal         the hash value of the block.
   * @return the size of serialized block.
   * @throws IOException if fail to write.
   */
  private long writeBlock(final long elementsInBlock,
                          final ByteArrayOutputStream outputStream,
                          final SerializedMemoryPartition partition,
                          final int hashVal) throws IOException {
    final byte[] serialized = outputStream.toByteArray();
    partition.writeBlock(hashVal, elementsInBlock, serialized);

    return serialized.length;
  }

  /**
   * Gets data coder from the {@link PartitionManagerWorker}.
   *
   * @param partitionId to get the coder.
   * @return the coder.
   */
  private Coder getCoderFromWorker(final String partitionId) {
    final PartitionManagerWorker worker = partitionManagerWorker.get();
    final String runtimeEdgeId = RuntimeIdGenerator.parsePartitionId(partitionId)[0];
    return worker.getCoder(runtimeEdgeId);
  }
}
