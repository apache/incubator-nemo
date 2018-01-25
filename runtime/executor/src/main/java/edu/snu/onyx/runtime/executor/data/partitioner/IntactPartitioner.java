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
package edu.snu.onyx.runtime.executor.data.partitioner;

import edu.snu.onyx.common.KeyExtractor;
import edu.snu.onyx.runtime.executor.data.NonSerializedPartition;
import edu.snu.onyx.runtime.executor.data.Partition;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * An implementation of {@link Partitioner} which makes an output data
 * from a source task to a single {@link Partition}.
 */
public final class IntactPartitioner implements Partitioner {

  @Override
  public List<Partition> partition(final Iterator elements,
                                   final int dstParallelism,
                                   final KeyExtractor keyExtractor) {
    return Collections.singletonList(new NonSerializedPartition(0, elements));
  }
}
