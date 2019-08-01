/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nemo.runtime.executor.data;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MemoryPoolAssignerTest {

  private MemoryPoolAssigner memoryPoolAssigner;
  private static final int MAX_MEM_MB = 1;
  private static final int CHUNK_SIZE_KB = 32;
  private static final int MAX_NUM_CHUNKS = MAX_MEM_MB * 1024 / CHUNK_SIZE_KB;
  private MemoryChunk chunk1, chunk2;

  @Before
  public void setUp() {
    this.memoryPoolAssigner = new MemoryPoolAssigner(1, 32);
  }

  @After
  public void cleanUp() {
    this.memoryPoolAssigner = null;
  }


  @Test(expected = MemoryAllocationException.class)
  public void TestTooMuchRequest() throws MemoryAllocationException {
    List<MemoryChunk> chunkList = new LinkedList<>();
    for (int i = 0; i < MAX_NUM_CHUNKS; i++) {
      chunkList.add(memoryPoolAssigner.allocateChunk());
    }
    memoryPoolAssigner.allocateChunk();
  }

  @Test
  public void TestReturnChunksToPool() throws MemoryAllocationException {
    List<MemoryChunk> chunkList = new LinkedList<>();
    for (int i = 0; i < MAX_NUM_CHUNKS; i++) {
      chunkList.add(memoryPoolAssigner.allocateChunk());
    }
    assertEquals(0, memoryPoolAssigner.returnPoolSize());
    memoryPoolAssigner.returnChunksToPool(chunkList);
    assertEquals(MAX_NUM_CHUNKS, memoryPoolAssigner.returnPoolSize());
  }

  @Test(expected = MemoryAllocationException.class)
  public void TestConcurrentAllocation() throws MemoryAllocationException {
    memoryPoolAssigner = new MemoryPoolAssigner(1, 32);
    final Thread chunkThread1 = new Thread(() -> {
      try{
        chunk1 = memoryPoolAssigner.allocateChunk();
      } catch (MemoryAllocationException e) {
        e.printStackTrace();
      }
    });
    final Thread chunkThread2 = new Thread(() -> {
      try{
        chunk2 = memoryPoolAssigner.allocateChunk();
      } catch (MemoryAllocationException e) {
        e.printStackTrace();
      }
    });
    try {
      for (int i = 0; i < MAX_NUM_CHUNKS / 2; i++) {
        chunkThread1.run();
        chunkThread2.run();
        chunkThread1.join();
        chunkThread2.join();
        Assert.assertFalse(chunk1.equals(chunk2));
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    memoryPoolAssigner.allocateChunk();
  }
}
