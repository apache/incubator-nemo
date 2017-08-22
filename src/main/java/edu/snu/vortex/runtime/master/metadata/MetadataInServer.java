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
package edu.snu.vortex.runtime.master.metadata;

import edu.snu.vortex.runtime.common.comm.ControlMessage;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class represents a metadata stored in the metadata server.
 * TODO #430: Handle Concurrency at Partition Level.
 * TODO #431: Include Partition Metadata in a Partition.
 */
@ThreadSafe
final class MetadataInServer {

  private AtomicBoolean written; // The whole data for this file is written or not yet.
  private final boolean hashed; // Each block in the corresponding file has a single hash value or not.
  private final List<ControlMessage.BlockMetadataMsg> blockMetadataList;

  MetadataInServer(final boolean hashed,
                   final List<ControlMessage.BlockMetadataMsg> blockMetadataList) {
    this.hashed = hashed;
    this.written = new AtomicBoolean(true);
    this.blockMetadataList = blockMetadataList;
  }

  /**
   * Gets the list of the block metadata.
   *
   * @return the list of block metadata.
   */
  List<ControlMessage.BlockMetadataMsg> getBlockMetadataList() {
    return Collections.unmodifiableList(blockMetadataList);
  }

  /**
   * Gets whether the whole data for this partition is written or not yet.
   *
   * @return whether the whole data for this partition is written or not yet.
   */
  boolean isWritten() {
    return written.get();
  }

  /**
   * Gets whether each block in the corresponding partition has a single hash value or not.
   *
   * @return whether each block in the corresponding partition has a single hash value or not.
   */
  boolean isHashed() {
    return hashed;
  }
}
