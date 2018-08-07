/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.nemo.runtime.executor.task;

import edu.snu.nemo.common.ir.Readable;
import edu.snu.nemo.common.ir.vertex.IRVertex;

import java.io.IOException;
import java.util.Iterator;

/**
 * Fetches data from a data source.
 */
class SourceVertexDataFetcher extends DataFetcher {
  private final Readable readable;

  // Non-finals (lazy fetching)
  private Iterator iterator;
  private long boundedSourceReadTime = 0;

  SourceVertexDataFetcher(final IRVertex dataSource,
                          final Readable readable,
                          final VertexHarness child,
                          final boolean isToSideInput) {
    super(dataSource, child, false, isToSideInput);
    this.readable = readable;
  }

  @Override
  boolean hasNext() {
    if (iterator == null) {
      getIteratorLazily();
    }

    return iterator.hasNext();
  }

  @Override
  Object next() {
    if (iterator == null) {
      getIteratorLazily();
    }

    return iterator.next();
  }

  void getIteratorLazily() {
    final long start = System.currentTimeMillis();
    try {
      iterator = this.readable.read().iterator();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    boundedSourceReadTime += System.currentTimeMillis() - start;
  }

  public final long getBoundedSourceReadTime() {
    return boundedSourceReadTime;
  }
}
