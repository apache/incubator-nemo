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

import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

/**
 * This class is a customized output stream implementation backed by
 * {@link ByteBuffer}, which utilizes off heap memory when writing the data via MemoryPoolAssigner.
 * Deletion of {@code dataList}, which is the memory this outputstream holds, occurs
 * when the corresponding block is deleted.
 */
public final class DirectByteBufferOutputStream extends OutputStream {

  private LinkedList<MemoryChunk> dataList = new LinkedList<>();
  private final int chunkSize;
  private ByteBuffer currentBuf;
  private final MemoryPoolAssigner memoryPoolAssigner;

  /**
   * Default constructor.
   *
   * @param memoryPoolAssigner  for memory allocation.
   * @throws MemoryAllocationException  if fails to allocate new memory.
   */
  public DirectByteBufferOutputStream(final MemoryPoolAssigner memoryPoolAssigner) throws MemoryAllocationException {
    this.chunkSize = memoryPoolAssigner.getChunkSize();
    this.memoryPoolAssigner = memoryPoolAssigner;
    newLastBuffer();
    currentBuf = dataList.getLast().getBuffer();
  }

  /**
   * Allocates new {@link ByteBuffer} with the capacity equal to {@code pageSize}.
   *
   * @throws MemoryAllocationException  if fail to allocate memory chunk.
   */
  private void newLastBuffer() throws MemoryAllocationException {
    dataList.addLast(memoryPoolAssigner.allocateChunk());
  }

  /**
   * Writes the specified byte to this output stream.
   *
   * @param   b   the byte to be written.
   */
  @Override
  public void write(final int b) throws IOException {
    try {
      if (currentBuf.remaining() <= 0) {
        newLastBuffer();
        currentBuf = dataList.getLast().getBuffer();
      }
      currentBuf.put((byte) b);
    } catch (IllegalStateException e) {
      throw new IllegalStateException("MemoryChunk has been freed");
    } catch (MemoryAllocationException e) {
      throw new IOException("Failed to allocate memory");
    }
  }

  /**
   * Writes {@code b.length} bytes from the specified byte array to this output stream.
   *
   * @param b the byte to be written.
   */
  @Override
  public void write(final byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  /**
   * Writes {@code len} bytes from the specified byte array
   * starting at offset {@code off} to this output stream.
   *
   * @param   b     the data.
   * @param   off   the start offset in the data.
   * @param   len   the number of bytes to write.
   */
  @Override
  public void write(final byte[] b, final int off, final int len) throws IOException {
    int byteToWrite = len;
    int offset = off;
    try {
      while (byteToWrite > 0) {
        if (currentBuf.remaining() <= 0) {
          newLastBuffer();
          currentBuf = dataList.getLast().getBuffer();
        }
        final int bufRemaining = currentBuf.remaining();
        if (bufRemaining < byteToWrite) {
          currentBuf.put(b, offset, bufRemaining);
          offset += bufRemaining;
          byteToWrite -= bufRemaining;
        } else {
          currentBuf.put(b, offset, byteToWrite);
          offset += byteToWrite;
          byteToWrite = 0;
        }
      }
    } catch (MemoryAllocationException e) {
      throw new IOException("Failed to allocate memory");
    }
  }


  /**
   * Creates a byte array that contains the whole content currently written in this output stream.
   *
   * USED BY TESTS ONLY.
   * @return the current contents of this output stream, as byte array.
   */
  @VisibleForTesting
  byte[] toByteArray() {
    if (dataList.isEmpty()) {
      final byte[] byteArray = new byte[0];
      return byteArray;
    }
    MemoryChunk lastBuf = dataList.getLast();
    // pageSize equals the size of the data filled in the ByteBuffers
    // except for the last ByteBuffer. The size of the data in the
    // ByteBuffer can be obtained by calling MemoryChunk.position().
    final int arraySize = chunkSize * (dataList.size() - 1) + lastBuf.getBuffer().position();
    final byte[] byteArray = new byte[arraySize];
    int start = 0;

    for (final MemoryChunk chunk : dataList) {
      // We use duplicated buffer to read the data so that there is no complicated
      // alteration of position and limit when switching between read and write mode.
      final MemoryChunk dupChunk = chunk.duplicate();
      final ByteBuffer dupBuffer = dupChunk.getBuffer();
      dupBuffer.flip();
      final int byteToWrite = dupBuffer.remaining();
      dupBuffer.get(byteArray, start, byteToWrite);
      start += byteToWrite;
    }

    return byteArray;
  }

  /**
   * Returns the list of {@code MemoryChunk}s that contains the written data.
   * List of flipped and duplicated {@link MemoryChunk}s are returned, which has independent
   * position and limit, to reduce erroneous data read/write.
   * This function has to be called when intended to read from the start of the list of
   * {@link MemoryChunk}s, not for additional write.
   *
   * @return the {@code LinkedList} of {@code MemoryChunk}s.
   */
  public List<MemoryChunk> getMemoryChunkList() {
    List<MemoryChunk> result = new LinkedList<>();
    for (final MemoryChunk chunk: dataList) {
      final MemoryChunk dupChunk = chunk.duplicate();
      dupChunk.getBuffer().flip();
      result.add(dupChunk);
    }
    return result;
  }

  /**
   * Returns the size of the data written in this output stream.
   *
   * @return the size of the data
   */
  public int size() {
    return chunkSize * (dataList.size() - 1) + dataList.getLast().getBuffer().position();
  }

  /**
   * Closing this output stream has no effect.
   */
  public void close() {
  }
}
