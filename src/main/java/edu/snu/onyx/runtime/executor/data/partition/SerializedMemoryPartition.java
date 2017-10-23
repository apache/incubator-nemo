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

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class represents a partition which is serialized and stored in local memory.
 */
@ThreadSafe
public final class SerializedMemoryPartition {

  private final List<SerializedBlock> blocks;
  private volatile AtomicBoolean committed;

  public SerializedMemoryPartition() {
    blocks = Collections.synchronizedList(new ArrayList<>());
    committed = new AtomicBoolean(false);
  }

  /**
   * Writes data to this partition as a block.
   *
   * @param key             the key value of this block.
   * @param elementsInBlock the number of elements in this block.
   * @param serializedData  the data to write.
   * @throws IOException if this partition is committed already.
   */
  public void writeBlock(final int key,
                         final long elementsInBlock,
                         final byte[] serializedData) throws IOException {
    if (!committed.get()) {
      blocks.add(new SerializedBlock(key, elementsInBlock, serializedData));
    } else {
      throw new IOException("Cannot append blocks to the committed partition");
    }
  }

  /**
   * @return the list of the blocks in this partition.
   */
  public List<SerializedBlock> getBlocks() {
    return blocks;
  }

  /**
   * Commits this partition to prevent further write.
   * If someone "subscribing" the data in this partition, it will be finished.
   */
  public void commit() {
    committed.set(true);
  }

  /**
   * A collection of serialized data.
   */
  public final class SerializedBlock {
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

    public int getKey() {
      return key;
    }

    public long getElementsInBlock() {
      return elementsInBlock;
    }

    public byte[] getSerializedData() {
      return serializedData;
    }
  }
}
