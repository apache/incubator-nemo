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

import java.nio.ByteBuffer;


/**
 * Supports both off-heap or on-heap. However it does not track if it is off-heap or on-heap.
 * It is just being used as it is assigned.
 */
public class MemoryChunk {

  private final ByteBuffer buffer;

  MemoryChunk(final ByteBuffer buffer) {
    this.buffer = buffer;
  }

  public ByteBuffer getBuffer() {
    return buffer;
  }

  public int remaining() {
    return buffer.remaining();
  }

  public ByteBuffer put(byte b) {
    return buffer.put(b);
  }

  public int position() {
    return buffer.position();
  }

  public MemoryChunk duplicate() {
    return new MemoryChunk(buffer.duplicate());
  }

  public void free() {
  }


}
