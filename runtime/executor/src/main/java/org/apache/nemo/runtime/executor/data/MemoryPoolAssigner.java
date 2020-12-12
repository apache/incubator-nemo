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

import net.jcip.annotations.ThreadSafe;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.nemo.conf.JobConf;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * The MemoryPoolAssigner assigns the memory that Nemo uses for writing data blocks from the {@link MemoryPool}.
 * Memory is represented in chunks of equal size. Consumers of off-heap memory acquire the memory by requesting
 * a number of {@link MemoryChunk} they need.
 *
 * MemoryPoolAssigner currently supports allocation of off-heap memory only.
 *
 * MemoryChunks are allocated on-demand, but if the total allocated memory exceeds the maxOffheapMb,
 * MemoryAllocationException is thrown and the job fails.
 * TODO #397: Separation of JVM heap region and off-heap memory region
 */
@ThreadSafe
public final class MemoryPoolAssigner {
  private final int chunkSize;

  private static final int MIN_CHUNK_SIZE_KB = 4;

  private final MemoryPool memoryPool;

  @Inject
  public MemoryPoolAssigner(@Parameter(JobConf.ExecutorMemoryMb.class) final int memory,
                            @Parameter(JobConf.MaxOffheapRatio.class) final double maxOffheapRatio,
                            @Parameter(JobConf.ChunkSizeKb.class) final int chunkSizeKb) {

    int maxOffheapMb = (int) (memory * maxOffheapRatio);
    if (chunkSizeKb < MIN_CHUNK_SIZE_KB) {
      throw new IllegalArgumentException("Chunk size too small. Minimum chunk size is 4KB");
    }
    final long maxNumChunks = (long) maxOffheapMb * 1024 / chunkSizeKb;
    if (maxNumChunks > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Too many pages to allocate (exceeds MAX_INT)");
    }
    if (maxNumChunks < 1) {
      throw new IllegalArgumentException("The given amount of memory amounted to less than one chunk.");
    }
    this.chunkSize = chunkSizeKb * 1024;
    this.memoryPool = new MemoryPool((int) maxNumChunks, this.chunkSize);
  }

  /**
   * Returns a single {@link MemoryChunk} from {@link MemoryPool}.
   *
   * @return a MemoryChunk
   * @throws MemoryAllocationException if fails to allocate MemoryChunk.
   */
  public MemoryChunk allocateChunk() throws MemoryAllocationException {
    return memoryPool.requestChunkFromPool();
  }

  /**
   * Returns all the MemoryChunks in the given List of MemoryChunks.
   *
   * @param target  the list of MemoryChunks to be returned to the memory pool.
   */
  public void returnChunksToPool(final Iterable<MemoryChunk> target) {
    for (final MemoryChunk chunk: target) {
      memoryPool.returnChunkToPool(chunk);
    }
  }

  /**
   * Returns the chunk size of the memory pool.
   *
   * @return the chunk size in bytes.
   */
  public int getChunkSize() {
    return chunkSize;
  }

  /**
   * Returns the number of chunks in the pool. This is unrecommended since it is very complex to
   * check the size of {@link ConcurrentLinkedQueue}.
   *
   * @return the pool size.
   */
  int poolSize() {
    return memoryPool.size();
  }

  /**
   * Memory pool that utilizes off-heap memory.
   * Supports pre-allocation of memory according to user specification.
   */
  @ThreadSafe
  private class MemoryPool {

    private final ConcurrentLinkedQueue<ByteBuffer> pool;
    private final int chunkSize;
    private long maxNumChunks;
    private long numChunks;

    MemoryPool(final long maxNumChunks, final int chunkSize) {
      this.chunkSize = chunkSize;
      this.pool = new ConcurrentLinkedQueue<>();
      this.maxNumChunks = maxNumChunks;
    }

    synchronized MemoryChunk allocateNewChunk() throws MemoryAllocationException {
      if (maxNumChunks <= numChunks) {
        throw new MemoryAllocationException("Exceeded maximum off-heap memory");
      }
      ByteBuffer memory = ByteBuffer.allocateDirect(chunkSize);
      numChunks += 1;
      return new MemoryChunk(memory);
    }

    MemoryChunk requestChunkFromPool() throws MemoryAllocationException {
      try {
        // Try to reuse a byte buffer in the current pool
        // Return the byte buffer if there's one that we can reuse
        final ByteBuffer byteBufferThatWeCanReuse = pool.remove(); // this can throw NoSuchElementException
        return new MemoryChunk(byteBufferThatWeCanReuse);
      } catch (final NoSuchElementException e) {
        // No more byte buffer to reuse in the current pool
        // So we try to allocate a new chunk
        // This method is synchronized :)
        return allocateNewChunk();
      } catch (final OutOfMemoryError e) {
        throw new MemoryAllocationException("Memory allocation failed due to lack of memory");
      }
    }

    /**
     * Returns MemoryChunk back to memory pool.
     *
     * @param chunk the target MemoryChunk to be returned to the pool.
     */
    void returnChunkToPool(final MemoryChunk chunk) {
      ByteBuffer buf = chunk.getBuffer();
      chunk.release();
      pool.add(buf);
    }

    int size() {
      return pool.size();
    }
  }
}
