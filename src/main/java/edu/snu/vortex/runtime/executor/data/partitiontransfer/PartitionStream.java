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
package edu.snu.vortex.runtime.executor.data.partitiontransfer;

import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.runtime.executor.data.HashRange;

import java.util.Optional;

/**
 * {@link PartitionInputStream} and {@link PartitionOutputStream}.
 */
public interface PartitionStream {

  /**
   * Gets the remote executor id.
   *
   * @return the remote executor id
   */
  String getRemoteExecutorId();

  /**
   * Gets whether the sender should start encoding even though the whole partition has not been written yet.
   *
   * @return whether the sender should start encoding even though the whole partition has not been written yet
   */
  boolean isEncodePartialPartitionEnabled();

  /**
   * Gets the partition store type.
   *
   * @return an {@link Optional} with partition store type if this stream belongs to a pull-based transfer,
   *         an empty {@link Optional} otherwise.
   */
  Optional<Attribute> getPartitionStore();

  /**
   * Gets the partition id.
   *
   * @return the partition id
   */
  String getPartitionId();

  /**
   * Gets the runtime edge id.
   *
   * @return the runtime edge id
   */
  String getRuntimeEdgeId();

  /**
   * Gets the hash range.
   *
   * @return the hash range
   */
  HashRange getHashRange();
}
