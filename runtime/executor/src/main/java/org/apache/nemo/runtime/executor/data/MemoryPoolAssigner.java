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

import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.nemo.conf.JobConf;

import javax.inject.Inject;
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

  private static final int MIN_CHUNK_SIZE = 4 * 1024; // 4KB

  private final MemoryPool memoryPool;

  private final int chunkSize;

  private final long memorySize;

  @Inject
  public MemoryPoolAssigner(@Parameter(JobConf.MemoryPoolSize.class) final long memorySize,
                            @Parameter(JobConf.ChunkSize.class) final int chunkSize) {
    if (chunkSize < MIN_CHUNK_SIZE) {
      throw new IllegalArgumentException("Chunk size too small. Minimum chunk size is 4KB");
    }
    this.memorySize = memorySize;
    this.chunkSize = chunkSize;
    final long numChunks = memorySize / chunkSize;
    if (numChunks > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Too many pages to allocate (exceeds MAX_INT)");
    }

    final int totalNumPages = (int) numChunks;
    if (totalNumPages < 1) {
      throw new IllegalArgumentException("The given amount of memory amounted to less than one page.");
    }

    this.memoryPool = new MemoryPool(totalNumPages, chunkSize);
  }

  /**
   * Returns list of {@link MemoryChunk}s to be used by consumers.
   *
   * @param numPages indicates the number of MemoryChunks
   * @param sequential whether the requested chunks are sequential.
   * @return list of {@link MemoryChunk}s
   * @throws MemoryAllocationException  if fail to allocate MemoryChunks.
   */
  public List<MemoryChunk> allocateChunks(final int numPages,
                                          final boolean sequential) throws MemoryAllocationException {
    final ArrayList<MemoryChunk> chunks = new ArrayList<MemoryChunk>(numPages);
    allocateChunks(chunks, numPages, sequential);
    return chunks;
  }

  /**
   * Allocates list of {@link MemoryChunk}s to target list.
   *
   * @param target    where the MemoryChunks are allocated
   * @param numChunks indicates the number of MemoryChunks
   * @param sequential  whether the requested chunks are sequential.
   * @throws MemoryAllocationException if fail to allocate MemoryChunks.
   */
  public void allocateChunks(final List<MemoryChunk> target, final int numChunks, final boolean sequential)
    throws MemoryAllocationException {

    // This function may cause significant overhead when the pool size is big.
    if (numChunks > (memoryPool.getNumOfAvailableMemoryChunks())) {
      throw new MemoryAllocationException("Could not allocate " + numChunks + " pages. Only "
        + (memoryPool.getNumOfAvailableMemoryChunks())
        + " pages are remaining.");
    }

    for (int i = numChunks; i > 0; i--) {
      MemoryChunk chunk = memoryPool.requestChunkFromPool(sequential);
      target.add(chunk);
    }
  }

  /**
   * Returns a single {@link MemoryChunk} from {@link MemoryPool}.
   *
   * @param sequential whether the requested chunk is sequential.
   * @return a MemoryChunk
   * @throws MemoryAllocationException if fails to allocate MemoryChunk.
   */
  public MemoryChunk allocateChunk(final boolean sequential) throws MemoryAllocationException {
    return memoryPool.requestChunkFromPool(sequential);
  }

  /**
   * Returns all the MemoryChunks in the given List of MemoryChunks.
   *
   * @param target  the list of MemoryChunks to be returned to the memory pool.
   */
  public void returnChunks(final List<MemoryChunk> target) {
    for (final MemoryChunk chunk: target) {
      memoryPool.returnChunkToPool(chunk);
    }
  }

  /**
   *
   * Supports off-heap memory pool.
   * off-heap is pre-allocated and managed. on-heap memory is used when off-heap memory runs out.
   *
   */
  private class MemoryPool {

    private final ConcurrentLinkedQueue<ByteBuffer> pool;
    private final int chunkSize;

    MemoryPool(final int numInitialChunks, final int chunkSize) {
      this.chunkSize = chunkSize;
      this.pool = new ConcurrentLinkedQueue<>();

      /** Pre-allocation of off-heap memory*/
      for (int i = 0; i < numInitialChunks; i++) {
        pool.add(ByteBuffer.allocateDirect(chunkSize));
      }
    }

    void allocateNewChunk() {
      ByteBuffer memory = ByteBuffer.allocateDirect(chunkSize);
      pool.add(memory);
    }

    MemoryChunk requestChunkFromPool(final boolean sequential) throws MemoryAllocationException {
      if (pool.isEmpty()) {
        allocateNewChunk();
      }
      ByteBuffer buf = pool.remove();
      return new MemoryChunk(buf, sequential);
    }

    /**
     * Only off-heap chunk is returned to the pool.
     *
     * @param chunk the target MemoryChunk to be returned to the pool.
     */
    void returnChunkToPool(final MemoryChunk chunk) {
      ByteBuffer buf = chunk.getBuffer();
      buf.clear();
      pool.add(buf);
      chunk.free();
    }

    /**
     * This function returns the currently available number of chunks in this memory pool.
     * Note that this function is not recommended to be used since the pool is implemented with
     * {@code ConcurrentLinkedQueue} that has complexity of O(n) hence it is hard to know the exact size.
     *
     * @return the remaining number of chunks the memory pool.
     */
    protected int getNumOfAvailableMemoryChunks() {
      return pool.size();
    }
  }
}
