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
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;


/**
 *
 */
public class DirectByteBufferOutputStream extends OutputStream {

  private ByteBuffer buf;
  private LinkedList<ByteBuffer> dataList = new LinkedList<>();
  private static final int pageSize = 4096;

  public DirectByteBufferOutputStream(){
    this(pageSize);
  }

  public DirectByteBufferOutputStream(int size){
    if (size < 0) {
      throw new IllegalArgumentException("Negative initial size: "
        + size);
    }
    this.buf = ByteBuffer.allocateDirect(size);
  }

  @Override
  public void write(int b){
    if (!buf.hasRemaining()) {
      throw new BufferOverflowException();
    }
    buf.put((byte) b);
  }

  /**
   *
   * @param b
   */
  @Override
  public void write(byte[] b){
    //TODO: Has to be implemented using list appending.

    int length = b.length;
    int offset = 0;

    buf.put(b, offset, pageSize);
    dataList.add(buf);
    offset += pageSize;
    length -= pageSize;

    while(length > 0){
      ByteBuffer temp = ByteBuffer.allocateDirect(pageSize);
      temp.put(b, offset, pageSize);
      offset += pageSize;
      length -= pageSize;
      dataList.add(temp);
    }
  }

  /**
   *
   * @param b
   * @param off
   * @param len
   */
  @Override
  public void write(byte[] b, int off, int len){
    //TODO: Has to be implemented using list appending.

    int length = len;
    int offset = off;

    buf.put(b, off, len);
    dataList.add(buf);
    offset += len;
    length -= len;

    while(length > 0){
      ByteBuffer temp = ByteBuffer.allocateDirect(pageSize);
      temp.put(b, offset, pageSize);
      offset += pageSize;
      length -= pageSize;
      dataList.add(temp);
    }
  }

  /**
   *
   */
  public void grow(){

    //TODO: Fill up the code to implement the mechanism of adding up the byte buffer when one is full.

  }

  /**
   *
   * @return
   */
  public byte[] toByteArray(){
    byte[] byteArray = new byte[pageSize * dataList.size()];
    int start = 0;

    while(!dataList.isEmpty()){
      ByteBuffer temp = dataList.remove(0);
      temp.get(byteArray, start, pageSize);
      start += pageSize;
    }
    return byteArray;
  }

  /**
   *
   * @return
   */
  public ByteBuffer getByteBuffer() { return buf; }

}
