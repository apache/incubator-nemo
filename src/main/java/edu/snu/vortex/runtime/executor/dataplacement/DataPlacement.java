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
package edu.snu.vortex.runtime.executor.dataplacement;

import edu.snu.vortex.compiler.ir.Element;

/**
 * Interface for various data placement mechanism specified in {@link edu.snu.vortex.runtime.common.RuntimeAttribute}.
 */
public interface DataPlacement {

  /**
   * Retrieves intermediate data corresponding to an edge in the physical plan being executed.
   * @param runtimeEdgeId of the edge between two tasks.
   * @param srcTaskIdx the index of the srcTask in its {@link edu.snu.vortex.runtime.common.plan.logical.RuntimeVertex}.
   * @return retrieved data.
   */
  Iterable<Element> get(final String runtimeEdgeId, final int srcTaskIdx);

  /**
   * Retrieves intermediate data corresponding to an edge in the physical plan being executed.
   * @param runtimeEdgeId of the edge between two tasks.
   * @param srcTaskIdx the index of the srcTask in its {@link edu.snu.vortex.runtime.common.plan.logical.RuntimeVertex}.
   * @param dstTaskIdx the index of the dstTask in its {@link edu.snu.vortex.runtime.common.plan.logical.RuntimeVertex}.
   *                   Used when only a partition of the data from srcTask is to be retrieved.
   * @return retrieved data.
   */
  Iterable<Element> get(final String runtimeEdgeId, final int srcTaskIdx, final int dstTaskIdx);

  /**
   * Puts intermediate data corresponding to an edge in the physical plan being executed.
   * @param runtimeEdgeId of the edge between two tasks.
   * @param srcTaskIdx the index of the srcTask in its {@link edu.snu.vortex.runtime.common.plan.logical.RuntimeVertex}.
   * @param data to put.
   */
  void put(final String runtimeEdgeId, final int srcTaskIdx, final Iterable<Element> data);

  /**
   * Puts intermediate data corresponding to an edge in the physical plan being executed.
   * @param runtimeEdgeId of the edge between two tasks.
   * @param srcTaskIdx the index of the srcTask in its {@link edu.snu.vortex.runtime.common.plan.logical.RuntimeVertex}.
   * @param partitionIdx the index of the partition this data should be written to.
   *                     Used when only a partition of the data is to be retrieved later on by the receiving task.
   * @param data to put.
   */
  void put(final String runtimeEdgeId, final int srcTaskIdx, final int partitionIdx, final Iterable<Element> data);
}
