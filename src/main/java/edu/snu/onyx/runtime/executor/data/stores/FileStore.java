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
package edu.snu.onyx.runtime.executor.data.stores;

import edu.snu.onyx.runtime.executor.data.FileArea;
import edu.snu.onyx.runtime.executor.data.HashRange;

import java.util.List;

/**
 * Stores partitions in (local or remote) files.
 */
public interface FileStore extends PartitionStore {

  /**
   * Gets the list of {@link FileArea}s for the specified partition.
   *
   * @param partitionId the partition id
   * @param hashRange   the hash range
   * @return the list of file areas
   */
  List<FileArea> getFileAreas(final String partitionId, final HashRange hashRange);
}
