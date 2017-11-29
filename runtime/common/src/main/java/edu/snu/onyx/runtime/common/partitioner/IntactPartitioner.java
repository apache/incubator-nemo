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
package edu.snu.onyx.runtime.common.partitioner;

import edu.snu.onyx.common.KeyExtractor;
import edu.snu.onyx.runtime.common.data.Block;

import java.util.Collections;
import java.util.List;

/**
 * An implementation of {@link Partitioner} which makes an output data from a source task to a single {@link Block}.
 */
public final class IntactPartitioner implements Partitioner {
  public static final String SIMPLE_NAME = "Intact";

  @Override
  public List<Block> partition(final Iterable elements,
                               final int dstParallelism,
                               final KeyExtractor keyExtractor) {
    return Collections.singletonList(new Block(elements));
  }
}
