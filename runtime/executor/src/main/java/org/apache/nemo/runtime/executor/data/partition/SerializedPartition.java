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
package org.apache.nemo.runtime.executor.data.partition;

import org.apache.nemo.runtime.executor.data.DirectByteBufferOutputStream;
import org.apache.nemo.runtime.executor.data.MemoryAllocationException;
import org.apache.nemo.runtime.executor.data.MemoryChunk;
import org.apache.nemo.runtime.executor.data.MemoryPoolAssigner;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.runtime.executor.data.streamchainer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import static org.apache.nemo.runtime.executor.data.DataUtil.buildOutputStream;

/**
 * A collection of data elements. The data is stored as an array of bytes.
 * This is a unit of read / write towards {@link org.apache.nemo.runtime.executor.data.block.Block}s.
 * Releasing the memory(either off-heap or on-heap) occurs on block deletion.
 * TODO #396: Refactoring SerializedPartition into multiple classes
 *
 * @param <K> the key type of its partitions.
 */
public final class SerializedPartition<K> implements Partition<byte[], K> {
  private static final Logger LOG = LoggerFactory.getLogger(SerializedPartition.class.getName());

  private final K key;
  private volatile byte[] serializedData;
  private volatile int length;
  private volatile boolean committed;
  // Will be null when the partition is committed when it is constructed.
  @Nullable
  private final DirectByteBufferOutputStream bytesOutputStream;
  @Nullable
  private final OutputStream wrappedStream;
  @Nullable
  private final EncoderFactory.Encoder encoder;
  private final MemoryPoolAssigner memoryPoolAssigner;
  private volatile List<MemoryChunk> dataList;
  private final boolean offheap;

  /**
   * Creates a serialized {@link Partition} without actual data.
   * Data can be written to this partition until it is committed.
   *
   * @param key        the key of this partition.
   * @param serializer the serializer to be used to serialize data.
   * @param memoryPoolAssigner  the memory pool assigner for memory allocation.
   * @throws IOException if fail to chain the output stream.
   * @throws MemoryAllocationException  if fail to allocate memory.
   */
  public SerializedPartition(final K key,
                             final Serializer serializer,
                             final MemoryPoolAssigner memoryPoolAssigner) throws IOException,
                                                                          MemoryAllocationException {
    this.key = key;
    this.serializedData = new byte[0];
    this.length = 0;
    this.committed = false;
    this.bytesOutputStream = new DirectByteBufferOutputStream(memoryPoolAssigner);
    this.wrappedStream = buildOutputStream(bytesOutputStream, serializer.getEncodeStreamChainers());
    this.encoder = serializer.getEncoderFactory().create(wrappedStream);
    this.memoryPoolAssigner = memoryPoolAssigner;
    this.offheap = true;
  }

  /**
   * Creates a serialized {@link Partition} with actual data residing in on-heap region.
   * Data cannot be written to this partition after the construction.
   *
   * @param key            the key.
   * @param serializedData the serialized data.
   * @param length         the length of the actual serialized data. (It can be different with serializedData.length)
   * @param memoryPoolAssigner the memory pool assigner.
   */
  public SerializedPartition(final K key,
                             final byte[] serializedData,
                             final int length,
                             final MemoryPoolAssigner memoryPoolAssigner) {
    this.key = key;
    this.serializedData = serializedData;
    this.length = length;
    this.committed = true;
    this.bytesOutputStream = null;
    this.wrappedStream = null;
    this.encoder = null;
    this.memoryPoolAssigner = memoryPoolAssigner;
    this.offheap = false;
  }

  /**
   * Creates a serialized {@link Partition} with actual data residing in off-heap region.
   * Data cannot be written to this partition after the construction.
   *
   * @param key                the key.
   * @param serializedChunkList the serialized data in list list of {@link MemoryChunk}s.
   * @param length             the length of the actual serialized data.(It can be different with serializedData.length)
   * @param memoryPoolAssigner  the memory pool assigner.
   */
  public SerializedPartition(final K key,
                             final List<MemoryChunk> serializedChunkList,
                             final int length,
                             final MemoryPoolAssigner memoryPoolAssigner) {
    this.key = key;
    this.dataList = serializedChunkList;
    this.length = length;
    this.committed = true;
    this.bytesOutputStream = null;
    this.wrappedStream = null;
    this.encoder = null;
    this.memoryPoolAssigner = memoryPoolAssigner;
    this.offheap = true;
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
        encoder.encode(element);
      } catch (final IOException e) {
        wrappedStream.close();
      }
    }
  }

  /**
   * Commits a partition to prevent further data write.
   *
   * @throws IOException if fail to commit partition.
   */
  @Override
  public void commit() throws IOException {
    if (!committed) {
      // We need to close wrappedStream on here, because DirectByteArrayOutputStream:getBufDirectly() returns
      // inner buffer directly, which can be an unfinished(not flushed) buffer.
      wrappedStream.close();
      this.dataList = bytesOutputStream.getMemoryChunkList();
      this.length = bytesOutputStream.size();
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
   * This method should only be used when this partition is residing in on-heap region.
   *
   * @return the serialized data.
   * @throws IOException if the partition is not committed yet.
   */
  @Override
  public byte[] getData() throws IOException {
    if (!committed) {
      throw new IOException("The partition is not committed yet!");
    } else if (offheap) {
      throw new RuntimeException("This partition does not have on-heap data");
    } else {
      return serializedData;
    }
  }

  /**
   * This method is used to emit the output as {@link SerializedPartition}.
   *
   * @return the serialized data in list of {@link ByteBuffer}s
   * @throws IOException if the partition is not committed yet.
   */
  public List<ByteBuffer> getDirectBufferList() throws IOException {
    if (!committed) {
      throw new IOException("The partition is not committed yet!");
    } else {
      List<ByteBuffer> result = new LinkedList<>();
      for (final MemoryChunk chunk : dataList) {
        final ByteBuffer dupBuffer = chunk.duplicate().getBuffer();
        result.add(dupBuffer);
      }
      return result;
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
   * @return whether this {@code SerializedPartition} is residing in off-heap region.
   */
  public boolean isOffheap() {
    return offheap;
  }

  /**
   * Releases the off-heap memory that this SerializedPartition holds.
   * TODO #403: Remove 'transient' uses of SerializedPartition.
   */
  public void release() {
    if (!committed) {
      throw new IllegalStateException("The partition is not committed yet!");
    }
    memoryPoolAssigner.returnChunksToPool(dataList);
  }
}
