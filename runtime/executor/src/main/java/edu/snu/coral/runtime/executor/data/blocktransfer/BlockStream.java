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
package edu.snu.coral.runtime.executor.data.blocktransfer;

import edu.snu.coral.common.ir.edge.executionproperty.DataStoreProperty;
import edu.snu.coral.runtime.common.data.KeyRange;

import java.util.Optional;

/**
 * {@link BlockInputStream} and {@link BlockOutputStream}.
 */
public interface BlockStream {

  /**
   * Gets the id of the remote executor.
   *
   * @return the remote executor id
   */
  String getRemoteExecutorId();

  /**
   * Gets whether the sender should start encoding even though the whole block has not been written yet.
   *
   * @return whether the sender should start encoding even though the whole block has not been written yet
   */
  boolean isEncodePartialBlockEnabled();

  /**
   * Gets the block store type.
   *
   * @return an {@link Optional} with block store type if this stream belongs to a pull-based transfer,
   *         an empty {@link Optional} otherwise.
   */
  Optional<DataStoreProperty.Value> getBlockStore();

  /**
   * Gets the block id.
   *
   * @return the block id
   */
  String getBlockId();

  /**
   * Gets the runtime edge id.
   *
   * @return the runtime edge id
   */
  String getRuntimeEdgeId();

  /**
   * Gets the key range.
   *
   * @return the {@link edu.snu.coral.runtime.common.data.KeyRange}
   */
  KeyRange getKeyRange();
}
