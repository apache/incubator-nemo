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


/**
 * OutputStream implementation backed by java.nio.ByteBuffer
 */
public class DirectByteBufferOutputStream extends OutputStream {

  private LinkedList<ByteBuffer> dataList = new LinkedList<>();
  private static final int pageSize = 4096;
  private int writePos = 0;

  public DirectByteBufferOutputStream(){
  }

  public void newLastBuffer() {
    dataList.addLast(ByteBuffer.allocateDirect(pageSize));
    writePos = 0;
  }

  @Override
  public void write(int b) {
    ByteBuffer currentBuf = (dataList.isEmpty() ? null: dataList.getLast());
    if (currentBuf == null || writePos >= pageSize){
      newLastBuffer();
      currentBuf = dataList.getLast();
    }
    currentBuf.put((byte)b);
    writePos += 1;
  }

  /**
   *
   * @param b
   */
  @Override
  public void write(byte[] b) {
    write(b, 0, b.length);
  }

  /**
   *
   * @param b
   * @param off
   * @param len
   */
  @Override
  public void write(byte[] b, int off, int len){
    int byteToWrite = len;
    int offset = off;

    ByteBuffer currentBuf = (dataList.isEmpty() ? null: dataList.getLast());
    while(byteToWrite > 0) {
      if (currentBuf == null || writePos >= pageSize){
        newLastBuffer();
        currentBuf = dataList.getLast();
      }
      final int bufRemaining = pageSize - writePos;
      if (bufRemaining < byteToWrite) {
        currentBuf.put(b, offset, bufRemaining);
        writePos += bufRemaining;
        offset += bufRemaining;
        byteToWrite -= bufRemaining;
      }
      else {
        currentBuf.put(b, offset, byteToWrite);
        writePos += byteToWrite;
        offset += byteToWrite;
        byteToWrite = 0;
      }
    }
  }

  /**
   *
   * @return
   */
  public byte[] toByteArray() {
    if (dataList.isEmpty()) {
      throw new IllegalStateException();
    }
    int arraySize = pageSize * (dataList.size() - 1) + writePos;
    byte[] byteArray = new byte[arraySize];
    int start = 0;
    ByteBuffer temp;

    while (!dataList.isEmpty()) {
      if(dataList.size()==1){
        temp = dataList.poll();
        temp.get(byteArray, start, writePos);
        start += writePos;
      }
      else{
        temp = dataList.poll();
        temp.get(byteArray, start, pageSize);
        start += pageSize;
      }
    }
    return byteArray;
  }
}
