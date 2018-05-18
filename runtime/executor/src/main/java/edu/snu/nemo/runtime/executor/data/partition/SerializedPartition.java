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
package edu.snu.nemo.runtime.executor.data.partition;

import edu.snu.nemo.common.DirectByteArrayOutputStream;
import edu.snu.nemo.common.coder.Coder;
import edu.snu.nemo.runtime.executor.data.streamchainer.Serializer;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;

import static edu.snu.nemo.runtime.executor.data.DataUtil.buildOutputStream;

/**
 * A collection of data elements. The data is stored as an array of bytes.
 * This is a unit of read / write towards {@link edu.snu.nemo.runtime.executor.data.block.Block}s.
 * @param <K> the key type of its partitions.
 */
public final class SerializedPartition<K> implements Partition<byte[], K> {
  private final K key;
  private volatile long elementsCount;
  private volatile byte[] serializedData;
  private volatile int length;
  private volatile boolean committed;
  // Will be null when the partition is committed when it is constructed.
  @Nullable private final DirectByteArrayOutputStream bytesOutputStream;
  @Nullable private final OutputStream wrappedStream;
  @Nullable private final Coder coder;

  /**
   * Creates a serialized {@link Partition} without actual data.
   * Data can be written to this partition until it is committed.
   *
   * @param key        the key of this partition.
   * @param serializer the serializer to be used to serialize data.
   * @throws IOException if fail to chain the output stream.
   */
  public SerializedPartition(final K key,
                             final Serializer serializer) throws IOException {
    this.key = key;
    this.elementsCount = 0;
    this.serializedData = new byte[0];
    this.length = 0;
    this.committed = false;
    this.bytesOutputStream = new DirectByteArrayOutputStream();
    this.wrappedStream = buildOutputStream(bytesOutputStream, serializer.getStreamChainers());
    this.coder = serializer.getCoder();
  }

  /**
   * Creates a serialized {@link Partition} with actual data.
   * Data cannot be written to this partition after the construction.
   *
   * @param key            the key.
   * @param elementsTotal  the total number of elements.
   * @param serializedData the serialized data.
   * @param length         the length of the actual serialized data. (It can be different with serializedData.length)
   */
  public SerializedPartition(final K key,
                             final long elementsTotal,
                             final byte[] serializedData,
                             final int length) {
    this.key = key;
    this.elementsCount = elementsTotal;
    this.serializedData = serializedData;
    this.length = length;
    this.committed = true;
    this.bytesOutputStream = null;
    this.wrappedStream = null;
    this.coder = null;
  }

  /**
   * Writes an element to non-committed partition.
   *
   * @param element element to write.
   * @throws IOException if the partition is already committed.
   */
  @Override
  public void write(final Object element) throws IOException {
    if (committed) {
      throw new IOException("The partition is already committed!");
    } else {
      try {
        coder.encode(element, wrappedStream);
        elementsCount++;
      } catch (final IOException e) {
        wrappedStream.close();
      }
    }
  }

  /**
   * Commits a partition to prevent further data write.
   * @throws IOException if fail to commit partition.
   */
  @Override
  public void commit() throws IOException {
    if (!committed) {
      // We need to close wrappedStream on here, because DirectByteArrayOutputStream:getBufDirectly() returns
      // inner buffer directly, which can be an unfinished(not flushed) buffer.
      wrappedStream.close();
      this.serializedData = bytesOutputStream.getBufDirectly();
      this.length = bytesOutputStream.getCount();
      this.committed = true;
    }
  }

  /**
   * @return the key value.
   */
  @Override
  public K getKey() {
    return key;
  }

  /**
   * @return whether the data in this {@link Partition} is serialized or not.
   */
  @Override
  public boolean isSerialized() {
    return true;
  }

  /**
   * @return the serialized data.
   * @throws IOException if the partition is not committed yet.
   */
  @Override
  public byte[] getData() throws IOException {
    if (!committed) {
      throw new IOException("The partition is not committed yet!");
    } else {
      return serializedData;
    }
  }

  /**
   * @return the length of the actual data.
   * @throws IOException if the partition is not committed yet.
   */
  public int getLength() throws IOException {
    if (!committed) {
      throw new IOException("The partition is not committed yet!");
    } else {
      return length;
    }
  }

  /**
   * @return the number of elements.
   * @throws IOException if the partition is not committed yet.
   */
  public long getElementsCount() throws IOException {
    if (!committed) {
      throw new IOException("The partition is not committed yet!");
    } else {
      return elementsCount;
    }
  }
}
