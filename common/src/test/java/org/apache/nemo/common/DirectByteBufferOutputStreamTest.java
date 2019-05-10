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

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests {@link DirectByteBufferOutputStream}.
 */
public class DirectByteBufferOutputStreamTest {
  private DirectByteBufferOutputStream outputStream;

  @Before
  public void setup(){
    outputStream = new DirectByteBufferOutputStream();
  }

  @Test
  public void testSingleWrite() {
    int value = 1;
    outputStream.write(value);
    assertEquals(value, outputStream.toByteArray()[0]);
  }

  @Test
  public void testWrite(){
    String value = "value";
    outputStream.write(value.getBytes());
    assertEquals(value, new String(outputStream.toByteArray()));
  }

  @Test
  public void testReWrite() {
    String value1 = "value1";
    String value2 = "value2";
    outputStream.write(value1.getBytes());
    assertEquals(value1, new String(outputStream.toByteArray()));
    outputStream.write(value2.getBytes());
    assertEquals(value1+value2, new String(outputStream.toByteArray()));
  }

  @Test
  public void testReRead() {
    String value = "value";
    outputStream.write(value.getBytes());
    assertEquals(value, new String(outputStream.toByteArray()));
    assertEquals(value, new String(outputStream.toByteArray()));
  }

  @Test
  public void testLongWrite() {
    String value = RandomStringUtils.randomAlphanumeric(10000);
    outputStream.write(value.getBytes());
    assertEquals(value,new String(outputStream.toByteArray()));
  }

  @Test
  public void testLongReWrite() {
    String value1 = RandomStringUtils.randomAlphanumeric(10000);
    String value2 = RandomStringUtils.randomAlphanumeric(5000);
    outputStream.write(value1.getBytes());
    assertEquals(value1, new String(outputStream.toByteArray()));
    outputStream.write(value2.getBytes());
    assertEquals(value1+value2, new String(outputStream.toByteArray()));
  }

  @Test
  public void testLongReRead() {
    String value = RandomStringUtils.randomAlphanumeric(10000);
    outputStream.write(value.getBytes());
    assertEquals(value, new String(outputStream.toByteArray()));
    assertEquals(value, new String(outputStream.toByteArray()));
  }

  @Test
  public void testGetBufferList() {
    String value = RandomStringUtils.randomAlphanumeric(10000);
    outputStream.write(value.getBytes());
    byte[] totalOutput = outputStream.toByteArray();
    List<ByteBuffer> bufList = outputStream.getBufferList();
    int offset = 0;
    int byteToRead;
    for (final ByteBuffer temp : bufList) {
      byteToRead = temp.remaining();
      byte[] output = new byte[byteToRead];
      temp.get(output, 0, byteToRead);
      byte[] expected = Arrays.copyOfRange(totalOutput, offset, offset+byteToRead);
      assertEquals(new String(expected), new String(output));
      offset += byteToRead;
    }
  }
}
