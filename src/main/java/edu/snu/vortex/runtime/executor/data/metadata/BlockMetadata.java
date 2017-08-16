/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.vortex.runtime.executor.data.metadata;

/**
 * This class represents a metadata for a block in a (local / remote) file partition.
 */
public final class BlockMetadata {
  private final int hashValue;
  private final int blockSize;
  private final long offset;
  private final long numElements;

  public BlockMetadata(final int hashValue,
                       final int blockSize,
                       final long offset,
                       final long numElements) {
    this.hashValue = hashValue;
    this.blockSize = blockSize;
    this.offset = offset;
    this.numElements = numElements;
  }

  public int getHashValue() {
    return hashValue;
  }

  public int getBlockSize() {
    return blockSize;
  }

  public long getOffset() {
    return offset;
  }

  public long getNumElements() {
    return numElements;
  }
}
