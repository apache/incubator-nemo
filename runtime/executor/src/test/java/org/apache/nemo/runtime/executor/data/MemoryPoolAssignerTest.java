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

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

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

  @Test(expected = MemoryAllocationException.class)
  public void testTooMuchRequest() throws MemoryAllocationException {
    List<MemoryChunk> chunkList = new LinkedList<>();
    for (int i = 0; i < MAX_NUM_CHUNKS; i++) {
      chunkList.add(memoryPoolAssigner.allocateChunk());
    }
    memoryPoolAssigner.allocateChunk();
  }

  @Test
  public void testReturnChunksToPool() {
    ConcurrentLinkedQueue<MemoryChunk> chunkList = new ConcurrentLinkedQueue();
    ExecutorService executor = Executors.newFixedThreadPool(5);
    CountDownLatch latch = new CountDownLatch(MAX_NUM_CHUNKS);
    for (int i = 0; i < MAX_NUM_CHUNKS; i++) {
      executor.submit(() -> {
        try {
          chunkList.add(memoryPoolAssigner.allocateChunk());
        } catch (MemoryAllocationException e) {
          throw new RuntimeException();
        }
        latch.countDown();
      });
    }
    try {
      latch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException();
    }
    assertEquals(0, memoryPoolAssigner.poolSize());
    memoryPoolAssigner.returnChunksToPool(chunkList);
    assertEquals(MAX_NUM_CHUNKS, memoryPoolAssigner.poolSize());
  }

  @Test(expected = MemoryAllocationException.class)
  public void testConcurrentAllocation() throws MemoryAllocationException {
    memoryPoolAssigner = new MemoryPoolAssigner(1, 32);
    final ExecutorService e1 = Executors.newSingleThreadExecutor();
    final ExecutorService e2 = Executors.newSingleThreadExecutor();
    for (int i = 0; i < MAX_NUM_CHUNKS / 2; i++) {
      CountDownLatch latch = new CountDownLatch(2);
      e1.submit(()->{
        try {
          chunk1 = memoryPoolAssigner.allocateChunk();
          latch.countDown();
        } catch (MemoryAllocationException e) {
          throw new RuntimeException();
        }
      });
      e2.submit(() -> {
        try{
          chunk2 = memoryPoolAssigner.allocateChunk();
          latch.countDown();
        } catch (MemoryAllocationException e) {
          throw new RuntimeException();
        }
      });
      try {
        latch.await();
      } catch (InterruptedException e) {
        throw new RuntimeException();
      }
      assertFalse(chunk1.equals(chunk2));
    }
    memoryPoolAssigner.allocateChunk();
  }
}
