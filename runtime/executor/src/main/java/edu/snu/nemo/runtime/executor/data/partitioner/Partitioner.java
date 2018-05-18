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

import java.io.Serializable;

/**
 * This interface represents the way of partitioning output data from a source task.
 * It takes an element and designates key of {@link edu.snu.nemo.runtime.executor.data.partition.Partition}
 * to write the element, according to the number of destination tasks, the key of each element, etc.
 * @param <K> the key type of the partition to write.
 */
public interface Partitioner<K extends Serializable> {

  /**
   * Divides the output data from a task into multiple blocks.
   *
   * @param element        the output element from a source task.
   * @return the key of the partition in the block to write the element.
   */
   K partition(Object element);
}
