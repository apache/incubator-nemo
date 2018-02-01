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
package edu.snu.coral.runtime.executor.data.partitioner;

import edu.snu.coral.common.KeyExtractor;
import edu.snu.coral.runtime.executor.data.NonSerializedPartition;
import edu.snu.coral.runtime.executor.data.Partition;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

/**
 * An implementation of {@link Partitioner} which hashes output data from a source task
 * according to the key of elements.
 * The data will be hashed by their key, and applied "modulo" operation by the number of destination tasks.
 */
public final class HashPartitioner implements Partitioner {

  @Override
  public List<Partition> partition(final Iterable elements,
                                   final int dstParallelism,
                                   final KeyExtractor keyExtractor) {
    final List<List> elementsByKey = new ArrayList<>(dstParallelism);
    IntStream.range(0, dstParallelism).forEach(dstTaskIdx -> elementsByKey.add(new ArrayList<>()));
    elements.forEach(element -> {
      // Hash the data by its key, and "modulo" by the number of destination tasks.
      final int dstIdx = Math.abs(keyExtractor.extractKey(element).hashCode() % dstParallelism);
      elementsByKey.get(dstIdx).add(element);
    });

    final List<Partition> partitions = new ArrayList<>(dstParallelism);
    for (int hashIdx = 0; hashIdx < dstParallelism; hashIdx++) {
      partitions.add(new NonSerializedPartition(hashIdx, elementsByKey.get(hashIdx)));
    }
    return partitions;
  }
}
