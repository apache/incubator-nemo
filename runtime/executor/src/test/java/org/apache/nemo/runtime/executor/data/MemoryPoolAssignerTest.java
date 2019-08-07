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

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;

public class MemoryPoolAssignerTest {
  private MemoryPoolAssigner memoryPoolAssigner;

  private static final Logger LOG = LoggerFactory.getLogger(MemoryPoolAssignerTest.class.getName());

  private static final int NUM_CONCURRENT_THREADS = 5;

  private static final int MAX_MEM_MB = 1;
  private static final int CHUNK_SIZE_KB = 32;
  private static final int MAX_NUM_CHUNKS = MAX_MEM_MB * 1024 / CHUNK_SIZE_KB;

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
  public void testConcurrentAllocReturnAlloc() throws InterruptedException {
    final ExecutorService executor = Executors.newFixedThreadPool(NUM_CONCURRENT_THREADS);
    for (int i = 0; i < MAX_NUM_CHUNKS; i++) {
      executor.submit(() -> {
        try {
          // We return this one immediately
          final MemoryChunk toReturnImmediately = memoryPoolAssigner.allocateChunk();
          memoryPoolAssigner.returnChunksToPool(Arrays.asList(toReturnImmediately));

          // We don't return this one
          memoryPoolAssigner.allocateChunk();
        } catch (MemoryAllocationException e) {
          throw new RuntimeException();
        }
      });
    }
    executor.shutdown();
    executor.awaitTermination(30, TimeUnit.SECONDS);
    assertEquals(0, memoryPoolAssigner.poolSize());
  }

  @Test
  public void testConcurrentUniqueAllocations() throws InterruptedException {
    final ExecutorService executorService = Executors.newFixedThreadPool(NUM_CONCURRENT_THREADS);
    final ConcurrentLinkedQueue<MemoryChunk> allocatedChunks = new ConcurrentLinkedQueue<>();

    for (int i = 0; i < MAX_NUM_CHUNKS; i++) {
      executorService.submit(()->{
        try {
          allocatedChunks.add(memoryPoolAssigner.allocateChunk());
        } catch (MemoryAllocationException e) {
          throw new RuntimeException();
        }
      });
    }
    executorService.shutdown();
    executorService.awaitTermination(30, TimeUnit.SECONDS);
    assertEquals(0, memoryPoolAssigner.poolSize());

    // All chunks should be unique
    assertEquals(allocatedChunks.size(), new HashSet<>(allocatedChunks).size());
  }
}
