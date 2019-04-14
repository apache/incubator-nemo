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

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.LinkedList;

/**
 * This class is a customized output stream implementation backed by
 * {@link ByteBuffer}, which utilizes off heap memory when writing the data.
 * Memory is allocated when needed by the specified {@code pageSize}.
 */
public class DirectByteBufferOutputStream extends OutputStream {

  private final LinkedList<ByteBuffer> dataList = new LinkedList<>();
  private final int pageSize;

  /**
   * Default constructor.
   * Sets the {@code pageSize} as default size of 4KB.
   */
  public DirectByteBufferOutputStream(){
    pageSize = 4096;
  }

  /**
   * Constructor specifying the {@code pageSize}.
   *
   * @param pageSize should be a power of 2 and greater than 4KB.
   */
  public DirectByteBufferOutputStream(final int pageSize){
    if(pageSize < 4096 || (pageSize & (pageSize - 1)) != 0){
      throw new IllegalArgumentException("Invalid pageSize");
    }
    this.pageSize = pageSize;
  }

  /**
   * Allocates new {@link ByteBuffer} with the capacity equal to {@code pageSize}.
   */
  public void newLastBuffer() {
    dataList.addLast(ByteBuffer.allocateDirect(pageSize));
  }

  /**
   * Writes the specified byte to this output stream.
   *
   * @param   b   the byte to be written.
   */
  @Override
  public void write(final int b) {
    ByteBuffer currentBuf = (dataList.isEmpty() ? null: dataList.getLast());
    if (currentBuf == null || currentBuf.remaining() <= 0){
      newLastBuffer();
      currentBuf = dataList.getLast();
    }
    currentBuf.put((byte)b);
  }

  /**
   * Writes {@code b.length} bytes from the specified byte array
   * to this output stream.
   *
   * @param b the byte to be written.
   */
  @Override
  public void write(final byte[] b) {
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
  public void write(final byte[] b, final int off, final int len){
    int byteToWrite = len;
    int offset = off;

    ByteBuffer currentBuf = (dataList.isEmpty() ? null: dataList.getLast());
    while(byteToWrite > 0) {
      if (currentBuf == null || currentBuf.remaining() <= 0){
        newLastBuffer();
        currentBuf = dataList.getLast();
      }
      final int bufRemaining = currentBuf.remaining();
      if (bufRemaining < byteToWrite) {
        currentBuf.put(b, offset, bufRemaining);
        offset += bufRemaining;
        byteToWrite -= bufRemaining;
      }
      else {
        currentBuf.put(b, offset, byteToWrite);
        offset += byteToWrite;
        byteToWrite = 0;
      }
    }
  }

  /**
   * Creates a byte array that contains the whole content
   * currently written in this output stream.
   *
   * @return the current contents of this output stream, as byte array.
   */
  public byte[] toByteArray() {
    if (dataList.isEmpty()) {
      final byte[] byteArray = new byte[0];
      return byteArray;
    }

    ByteBuffer lastBuf = dataList.getLast();
    // pageSize equals the size of the data filled in the ByteBuffers
    // except for the last ByteBuffer. The size of the data in the last ByteBuffer
    // can be obtained by calling ByteBuffer.position().
    final int arraySize = pageSize * (dataList.size() - 1) + lastBuf.position();
    final byte[] byteArray = new byte[arraySize];
    int start = 0;
    int byteToWrite;

    for(ByteBuffer temp : dataList) {
      // ByteBuffer has to be shifted to read mode by calling ByteBuffer.flip(),
      // which sets its limit to the current position and sets the position to 0.
      // Note that capacity remains the same.
      temp.flip();
      byteToWrite = temp.remaining();
      temp.get(byteArray, start, byteToWrite);
      start += byteToWrite;
    }
    // The limit of the last buffer has to be set to the capacity for re-write.
    lastBuf.limit(lastBuf.capacity());

    return byteArray;
  }

  /**
   * Closes this output stream and releases resources.
   */
  @Override
  public void close() {
    dataList.clear();
  }
}
