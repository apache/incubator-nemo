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
package org.apache.nemo.common;

import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * This class is a customized output stream implementation backed by
 * {@link ByteBuffer}, which utilizes off heap memory when writing the data.
 * Memory is allocated when needed by the specified {@code pageSize}.
 * Deletion of {@code dataList}, which is the memory this outputstream holds, occurs
 * when the corresponding block is deleted.
 * TODO #388: Off-heap memory management (reuse ByteBuffer) - implement reuse.
 */
public final class DirectByteBufferOutputStream extends OutputStream {

  private LinkedList<MemoryChunk> dataList = new LinkedList<>();
  private static final int DEFAULT_PAGE_SIZE = 32768; //32KB
  private final int pageSize;
  //private ByteBuffer currentBuf;
  private MemoryChunk currentBuf;
  private final MemoryPoolAssigner memoryPoolAssigner;

  /**
   * Default constructor.
   * Sets the {@code pageSize} as default size of 4096 bytes.
   */
  public DirectByteBufferOutputStream(final MemoryPoolAssigner memoryPoolAssigner) {
    this(DEFAULT_PAGE_SIZE, memoryPoolAssigner);
  }

  /**
   * Constructor which sets {@code pageSize} as specified {@code size}.
   * Note that the {@code pageSize} has trade-off between memory fragmentation and
   * native memory (de)allocation overhead.
   *
   * @param size should be a power of 2 and greater than or equal to 4096.
   */
  public DirectByteBufferOutputStream(final int size, final MemoryPoolAssigner memoryPoolAssigner) {
    if (size < DEFAULT_PAGE_SIZE || (size & (size - 1)) != 0) {
      throw new IllegalArgumentException("Invalid pageSize");
    }
    this.pageSize = size;
    this.memoryPoolAssigner = memoryPoolAssigner;
    newLastBuffer();
    currentBuf = dataList.getLast();
  }

  /**
   * Allocates new {@link ByteBuffer} with the capacity equal to {@code pageSize}.
   */
  // TODO #388: Off-heap memory management (reuse ByteBuffer)
  private void newLastBuffer() {
    dataList.addLast(memoryPoolAssigner.allocateChunk(true));
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
        currentBuf = dataList.getLast();
      }
      currentBuf.put((byte) b);
    } catch (IllegalStateException e) {
      throw new IllegalStateException("MemoryChunk has been freed");
    } catch (IllegalAccessException e) {
      throw new IOException("Not allowed for non-sequential MemoryChunk");
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
          currentBuf = dataList.getLast();
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
    } catch (IllegalAccessException e) {
      throw new IOException("Not allowed for non-sequential MemoryChunk");
    }
  }


  /**
   * Creates a byte array that contains the whole content currently written in this output stream.
   *
   * USED BY TESTS ONLY.
   * @return the current contents of this output stream, as byte array.
   */
  @VisibleForTesting
  byte[] toByteArray() throws IllegalAccessException {
    if (dataList.isEmpty()) {
      final byte[] byteArray = new byte[0];
      return byteArray;
    }

    //ByteBuffer lastBuf = dataList.getLast();
    MemoryChunk lastBuf = dataList.getLast();
    // pageSize equals the size of the data filled in the ByteBuffers
    // except for the last ByteBuffer. The size of the data in the
    // ByteBuffer can be obtained by calling ByteBuffer.position().
    final int arraySize = pageSize * (dataList.size() - 1) + lastBuf.position();
    final byte[] byteArray = new byte[arraySize];
    int start = 0;

    for (final MemoryChunk buffer : dataList) {
      // We use duplicated buffer to read the data so that there is no complicated
      // alteration of position and limit when switching between read and write mode.
      final MemoryChunk dupChunk = buffer.duplicate();
      final ByteBuffer dupBuffer = dupChunk.getBuffer();
      dupBuffer.flip();
      final int byteToWrite = dupBuffer.remaining();
      dupBuffer.get(byteArray, start, byteToWrite);
      start += byteToWrite;
    }

    return byteArray;
  }

  /**
   * Returns the list of {@code ByteBuffer}s that contains the written data.
   * List of flipped and duplicated {@link ByteBuffer}s are returned which has independent
   * position and limit, to reduce erroneous data read/write.
   * This function has to be called when intended to read from the start of the list of
   * {@link ByteBuffer}s, not for additional write.
   *
   * @return the {@code LinkedList} of {@code ByteBuffer}s.
   */
  public List<ByteBuffer> getDirectByteBufferList() {
    List<ByteBuffer> result = new ArrayList<>(dataList.size());
    for (final MemoryChunk chunk : dataList) {
      final MemoryChunk dupChunk = chunk.duplicate();
      final ByteBuffer dupBuffer = dupChunk.getBuffer();
      dupBuffer.flip();
      result.add(dupBuffer);
    }
    return result;
  }

  /**
   * Returns the size of the data written in this output stream.
   *
   * @return the size of the data
   */
  public int size() throws IllegalAccessException {
    return pageSize * (dataList.size() - 1) + dataList.getLast().position();
  }

  public void release() {
    memoryPoolAssigner.returnChunks(dataList);
  }

  /**
   * Closing this output stream has no effect.
   */
  public void close() {
  }
}
