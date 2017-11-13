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
package edu.snu.onyx.runtime.executor.data.partition;

import edu.snu.onyx.common.coder.Coder;
import edu.snu.onyx.runtime.executor.data.Block;
import edu.snu.onyx.runtime.executor.data.DataUtil;
import edu.snu.onyx.runtime.executor.data.HashRange;

import javax.annotation.concurrent.ThreadSafe;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * This class represents a partition which is serialized and stored in local memory.
 */
@ThreadSafe
public final class SerializedMemoryPartition implements Partition {

  private final List<SerializedBlock> blocks;
  private final Coder coder;
  private volatile boolean committed;

  public SerializedMemoryPartition(final Coder coder) {
    this.coder = coder;
    blocks = new ArrayList<>();
    committed = false;
  }

  /**
   * Writes {@link Block}s to this partition.
   * Invariant: This should not be invoked after this partition is committed.
   *
   * @param blocksToWrite the {@link Block}s to write.
   * @throws IOException if fail to write.
   */
  @Override
  public synchronized Optional<List<Long>> putBlocks(final Iterable<Block> blocksToWrite) throws IOException {
    if (!committed) {
      final List<Long> blockSizeList = new ArrayList<>();
      // Serialize the given blocks
      try (final ByteArrayOutputStream bytesOutputStream = new ByteArrayOutputStream()) {
        for (final Block block : blocksToWrite) {
          final long elementsTotal = DataUtil.serializeBlock(coder, block, bytesOutputStream);

          // Write the serialized block.
          final byte[] serialized = bytesOutputStream.toByteArray();
          blocks.add(new SerializedBlock(block.getKey(), elementsTotal, serialized));
          blockSizeList.add((long) serialized.length);
          bytesOutputStream.reset();
        }
      }

      return Optional.of(blockSizeList);
    } else {
      throw new IOException("Cannot append blocks to the committed partition");
    }
  }

  /**
   * Retrieves the blocks in a specific hash range and deserializes it from this partition.
   * Invariant: This should not be invoked before this partition is committed.
   *
   * @param hashRange the hash range to retrieve.
   * @return an iterable of deserialized blocks.
   * @throws IOException if failed to deserialize.
   */
  @Override
  public Iterable<Block> getBlocks(final HashRange hashRange) throws IOException {
    if (committed) {
      final List<Block> deserializedBlock = new ArrayList<>();
      for (final SerializedBlock serializedBlock : blocks) {
        final int hashVal = serializedBlock.getKey();
        if (hashRange.includes(hashVal)) {
          final List deserializedData = new ArrayList<>();
          // The hash value of this block is in the range.
          try (final ByteArrayInputStream byteArrayInputStream =
                   new ByteArrayInputStream(serializedBlock.getSerializedData())) {
            DataUtil.deserializeBlock(
                serializedBlock.getElementsInBlock(), coder, byteArrayInputStream, deserializedData);
          }
          deserializedBlock.add(new Block(hashVal, deserializedData));
        }
      }

      return deserializedBlock;
    } else {
      throw new IOException("Cannot retrieve elements before a partition is committed");
    }
  }

  /**
   * Retrieves the elements in a specific hash range as serialized bytes from this partition.
   * Constraint: This should be invoked after this partition is committed.
   *
   * @param hashRange the hash range to retrieve.
   * @return an iterable of serialized elements.
   * @throws IOException if the partition is not committed yet.
   */
  public Iterable<byte[]> retrieveSerializedElements(final HashRange hashRange) throws IOException {
    if (committed) {
      final List<byte[]> serializedData = new ArrayList<>();
      blocks.forEach(serializedBlock -> {
        final int hashVal = serializedBlock.getKey();
        if (hashRange.includes(hashVal)) {
          // The hash value of this block is in the range.
          serializedData.add(serializedBlock.getSerializedData());
        }
      });

      return serializedData;
    } else {
      throw new IOException("Cannot retrieve elements before a partition is committed");
    }
  }

  /**
   * Commits this partition to prevent further write.
   */
  @Override
  public synchronized void commit() {
    committed = true;
  }

  /**
   * A collection of serialized data.
   */
  private final class SerializedBlock {
    private final int key;
    private final long elementsInBlock;
    private final byte[] serializedData;

    private SerializedBlock(final int key,
                            final long elementsInBlock,
                            final byte[] serializedData) {
      this.key = key;
      this.elementsInBlock = elementsInBlock;
      this.serializedData = serializedData;
    }

    private int getKey() {
      return key;
    }

    private long getElementsInBlock() {
      return elementsInBlock;
    }

    private byte[] getSerializedData() {
      return serializedData;
    }
  }
}
