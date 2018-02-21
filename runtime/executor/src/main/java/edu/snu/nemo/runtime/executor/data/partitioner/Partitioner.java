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
package edu.snu.nemo.runtime.executor.data.partitioner;

import edu.snu.nemo.common.KeyExtractor;
import edu.snu.nemo.runtime.executor.data.Partition;

import java.util.List;

/**
 * This interface represents the way of partitioning output data from a source task.
 * It takes an iterable of elements and divide the data into multiple {@link Partition}s,
 * according to the number of destination tasks, the key of each element, etc.
 */
public interface Partitioner {

  /**
   * Divides the output data from a task into multiple blocks.
   *
   * @param elements       the output data from a source task.
   * @param dstParallelism the number of destination tasks.
   * @param keyExtractor   extracts keys from elements.
   * @return the list of partitioned blocks.
   */
  List<Partition> partition(Iterable elements, int dstParallelism, KeyExtractor keyExtractor);
}
