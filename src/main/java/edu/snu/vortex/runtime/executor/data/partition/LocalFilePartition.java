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
package edu.snu.vortex.runtime.executor.data.partition;

import edu.snu.vortex.common.coder.Coder;
import edu.snu.vortex.runtime.executor.data.metadata.FileMetadata;

import java.io.*;

/**
 * This class implements the {@link FilePartition} which is stored in a local file.
 * This partition have to be treated as an actual file
 * (i.e., construction and removal of this partition means the creation and deletion of the file),
 * even though the actual data is stored only in the local disk.
 * Also, to prevent the memory leak, this partition have to be closed when any exception is occurred during write.
 */
public final class LocalFilePartition extends FilePartition {

  /**
   * Constructs a local file partition.
   * Corresponding {@link FilePartition#finishWrite()} is required.
   *
   * @param coder    the coder used to serialize and deserialize the data of this partition.
   * @param filePath the path of the file which will contain the data of this partition.
   * @param metadata     the metadata for this partition.
   * @throws IOException if fail to open this partition to write.
   */
  public LocalFilePartition(final Coder coder,
                            final String filePath,
                            final FileMetadata metadata) throws IOException {
    super(coder, filePath, metadata);
    openFileStream();
  }
}
