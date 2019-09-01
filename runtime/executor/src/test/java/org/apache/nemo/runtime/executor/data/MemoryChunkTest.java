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

import org.apache.commons.lang.ArrayUtils;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests basic operations on {@link MemoryChunk}.
 */
public class MemoryChunkTest {
  private MemoryChunk chunk;
  private ByteBuffer buffer = ByteBuffer.allocateDirect(1024);

  @Before
  public void setup() {
    chunk = new MemoryChunk(buffer);
  }

  @Test
  public void testPutAndGet() {
    byte input = 5;
    chunk.put(0, input);
    assertEquals(input, chunk.get(0));
  }

  @Test
  public void testPutAndGetByArray() {
    byte[] input = new byte[1000]; new Random().nextBytes(input);
    chunk.put(0, input);
    byte[] output = new byte[1000]; chunk.get(0, output);
    assertArrayEquals(input, output);
  }

  @Test
  public void testBulkPutAndGet() {
    byte[] input1 = new byte[1000]; new Random().nextBytes(input1);
    byte[] input2 = new byte[24]; new Random().nextBytes(input2);
    chunk.put(0, input1);
    chunk.put(input1.length, input2);
    byte[] output = new byte[1024]; chunk.get(0, output);
    byte[] concat = ArrayUtils.addAll(input1, input2);
    assertArrayEquals(concat, output);
  }

  @Test
  public void testPutCharAndGetChar() {
    char input = 'a';
    chunk.putChar(0,input);
    assertEquals(input, chunk.getChar(0));
  }

  @Test
  public void testPutShortAndGetShort() {
    short input = 1;
    chunk.putShort(0, input);
    assertEquals(input, chunk.getShort(0));
  }

  @Test
  public void testPutIntAndGetInt() {
    int input = 14;
    chunk.putInt(0, input);
    assertEquals(input, chunk.getInt(0));
  }

  @Test
  public void testPutLongAndGetLong() {
    long input = 28;
    chunk.putLong(0, input);
    assertEquals(input, chunk.getLong(0));
  }

  @Test
  public void testPutFloatAndGetFloat() {
    float input = 3;
    chunk.putFloat(0, input);
    assertEquals(input, chunk.getFloat(0), 0.1);
  }

  @Test
  public void testPutDoubleAndGetDouble() {
    double input = 3.7;
    chunk.putDouble(0, input);
    assertEquals(input, chunk.getDouble(0), 0.01);
  }
}
