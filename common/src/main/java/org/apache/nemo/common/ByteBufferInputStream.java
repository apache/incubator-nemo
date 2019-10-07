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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * This class is a customized input stream implementation which reads data from
 * list of {@link ByteBuffer}. If the {@link ByteBuffer} is direct, it may reside outside
 * the normal garbage-collected heap memory.
 */
public final class ByteBufferInputStream extends InputStream {
  private List<ByteBuffer> bufList;
  private int current = 0;
  private static final int BITMASK = 0xff;

  /**
   * Default Constructor.
   *
   * @param bufList is the target data to read.
   */
  public ByteBufferInputStream(final List<ByteBuffer> bufList) {
   this.bufList = bufList;
  }

  /**
   * Reads data from the list of {@code ByteBuffer}s.
   *
   * @return integer.
   * @throws IOException exception.
   */
  @Override
  public int read() throws IOException {
    // Since java's byte is signed type, we have to mask it to make byte
    // become unsigned type to properly retrieve `int` from sequence of bytes.
    return getBuffer().get() & BITMASK;
  }

  /**
   * Return next non-empty @code{ByteBuffer}.
   *
   * @return {@code ByteBuffer} to write the data.
   * @throws IOException when fail to retrieve buffer.
   */
  public ByteBuffer getBuffer() throws IOException {
    while (current < bufList.size()) {
      ByteBuffer buffer = bufList.get(current);
      if (buffer.hasRemaining()) {
        return buffer;
      }
      current += 1;
    }
    throw new EOFException();
  }
}
