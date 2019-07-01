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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * The MemoryPoolAssigner assigns the memory that Nemo uses for writing data blocks from the {@link MemoryPool}.
 * Memory is represented in chunks of equal size. Consumers of off-heap memory acquire the memory by requesting
 * a number of {@link MemoryChunk} they need.
 *
 * MemoryPoolAssigner currently supports allocation of off-heap memory only.
 *
 * The MemoryPoolAssigner pre-allocates all memory at the start. Memory will be occupied and reserved from start on,
 * which means that no OutOfMemoryError comes while requesting memory. Released memory will return to the MemoryPool.
 */
public class MemoryPoolAssigner {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryPoolAssigner.class.getName());
  public static final int DEFAULT_PAGE_SIZE = 32 * 1024;
  public static final int MIN_PAGE_SIZE = 4 * 1024;
  private final MemoryPool memoryPool;
  private final int pageSize;
  private final long memorySize;



  public MemoryPoolAssigner(final long memorySize) {
    this(memorySize, DEFAULT_PAGE_SIZE);
  }

  public MemoryPoolAssigner(final long memorySize, final int pageSize) {
    this.memorySize = memorySize;
    this.pageSize = pageSize;
    final long numPages = memorySize / pageSize;
    if (numPages > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("The given number of memory bytes (" + memorySize
        + ") corresponds to more than MAX_INT pages.");
    }

    final int totalNumPages = (int) numPages;
    if (totalNumPages < 1) {
      throw new IllegalArgumentException("The given amount of memory amounted to less than one page.");
    }

    this.memoryPool = new MemoryPool(totalNumPages, pageSize);
  }

  public List<MemoryChunk> allocatePages(int numPages) throws MemoryAllocationException {
    final ArrayList<MemoryChunk> segs = new ArrayList<MemoryChunk>(numPages);
    allocatePages(segs, numPages);
    return segs;
  }

  public void allocatePages(final List<MemoryChunk> target, final int numPages)
    throws MemoryAllocationException {

    if (numPages > (memoryPool.getNumOfAvailableMemoryChunks())) {
      throw new MemoryAllocationException("Could not allocate " + numPages + " pages. Only " +
        (memoryPool.getNumOfAvailableMemoryChunks())
        + " pages are remaining.");
    }

    for (int i = numPages; i > 0; i--) {
      MemoryChunk chunk = memoryPool.requestChunkFromPool();
      target.add(chunk);
    }
  }


//  abstract static class MemoryPool {
//    abstract int getNumOfAvailableMemoryChunks();
//
//    abstract MemoryChunk allocateNewChunk();
//
//    abstract MemoryChunk requestChunkFromPool();
//
//    abstract void returnChunkToPool(MemoryChunk segment);
//
//    abstract void clear();
//  }


//   static final class HeapMemoryPool extends MemoryPool {
//   private final ConcurrentLinkedQueue<ByteBuffer> available;
//   private final int chunkSize;
//   HeapMemoryPool(final int numInitialChunks, final int chunkSize) {
//   this.chunkSize = chunkSize;
//   this.available = new ConcurrentLinkedQueue<>();
//   for (int i = 0; i < numInitialChunks; i++) {
//   this.available.add(ByteBuffer.allocate(chunkSize));
//   }
//   }
//   @Override
//   int getNumOfAvailableMemoryChunks() {
//   return this.available.size();
//   }
//   @Override
//   MemoryChunk allocateNewChunk() {
//   ByteBuffer memory = ByteBuffer.allocate(chunkSize);
//   return new MemoryChunk(memory);
//   }
//   @Override
//   MemoryChunk requestChunkFromPool() {
//   }
//   abstract void returnChunkToPool(MemoryChunk segment);
//   abstract void clear();
//   }

  /**
   *
   * Supports both on-heap and off-heap memory pool.
   * off-heap is pre-allocated and managed. on-heap memory is used when off-heap memory runs out.
   *
   */
  static final class MemoryPool {

    private final ConcurrentLinkedQueue<ByteBuffer> available;
    private final int chunkSize;

    MemoryPool(final int numInitialChunks, final int chunkSize) {
      this.chunkSize = chunkSize;
      this.available = new ConcurrentLinkedQueue<>();

      /** Pre-allocation of off-heap memory*/
      for (int i = 0; i < numInitialChunks; i++) {
        this.available.add(ByteBuffer.allocateDirect(chunkSize));
      }
    }

    MemoryChunk allocateNewOffHeapChunk() {
      ByteBuffer memory = ByteBuffer.allocateDirect(chunkSize);
      return new MemoryChunk(memory);
    }

    /**
     * Used when there is no available buffer in the pool.
     * @return
     */
    MemoryChunk allocateNewOnHeapChunk() {
      ByteBuffer memory = ByteBuffer.allocate(chunkSize);
      return new MemoryChunk(memory);
    }

    MemoryChunk requestChunkFromPool() {
      ByteBuffer buf = available.remove();
      return new MemoryChunk(buf);
    }

    /**
     * Only off-heap chunk is returned to the pool.
     * On-heap chunk is not managed as a pool actually.
     * @param chunk
     */
    void returnChunkToPool(final MemoryChunk chunk) {
      MemoryChunk offHeapChunk = chunk;
      ByteBuffer buf = offHeapChunk.getBuffer();
      available.add(buf);
      chunk.free();
    }

    protected int getNumOfAvailableMemoryChunks() {
      return available.size();
    }

    void clear() {
      available.clear();
    }
  }
}
