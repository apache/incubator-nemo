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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class represents a metadata for a local file partition.
 * It resides in local only, and does not needed to be synchronized.
 */
public final class LocalFileMetadata extends FileMetadata {

  private final AtomicBoolean written; // The whole data for this partition is written or not yet.

  public LocalFileMetadata(final boolean hashed) {
    super(hashed);
    written = new AtomicBoolean(false);
  }

  /**
   * Gets whether the whole data for this partition is written or not yet.
   *
   * @return whether the whole data for this partition is written or not yet.
   */
  @Override
  public boolean isWritten() {
   return written.get();
  }

  /**
   * Marks that the whole data for this partition is written.
   * It does not do any synchronization, because this metadata is for a local file.
   *
   * @return {@code true} if already set, or {@code false} if not.
   */
  @Override
  public boolean getAndSetWritten() {
    return written.getAndSet(true);
  }

  /**
   * @see FileMetadata#deleteMetadata().
   */
  @Override
  public void deleteMetadata() {
    // Do nothing because this metadata is only in the local memory.
  }
}
