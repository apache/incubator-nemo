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
package edu.snu.onyx.runtime.executor.datatransfer.partitioning;

import edu.snu.onyx.compiler.ir.Element;
import edu.snu.onyx.runtime.executor.data.Block;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

/**
 * An implementation of {@link Partitioner} which hashes output data from a source task
 * according to the key of elements.
 * The data will be hashed by their key, and applied "modulo" operation by the number of destination tasks.
 */
public final class HashPartitioner implements Partitioner {
  public static final String SIMPLE_NAME = "Hash";

  @Override
  public List<Block> partition(final Iterable<Element> elements,
                               final int dstParallelism) {
    final List<List<Element>> elementsByKey = new ArrayList<>(dstParallelism);
    IntStream.range(0, dstParallelism).forEach(dstTaskIdx -> elementsByKey.add(new ArrayList<>()));
    elements.forEach(element -> {
      // Hash the data by its key, and "modulo" by the number of destination tasks.
      final int dstIdx = Math.abs(element.getKey().hashCode() % dstParallelism);
      elementsByKey.get(dstIdx).add(element);
    });

    final List<Block> blocks = new ArrayList<>(dstParallelism);
    for (int hashIdx = 0; hashIdx < dstParallelism; hashIdx++) {
      blocks.add(new Block(hashIdx, elementsByKey.get(hashIdx)));
    }
    return blocks;
  }
}
