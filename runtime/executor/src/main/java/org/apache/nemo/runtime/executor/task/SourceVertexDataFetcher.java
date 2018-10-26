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
package org.apache.nemo.runtime.executor.task;

import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.vertex.IRVertex;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

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
                          final OutputCollector outputCollector) {
    super(dataSource, outputCollector);
    this.readable = readable;
  }

  @Override
  Object fetchDataElement() throws IOException {
    if (iterator == null) {
      fetchDataLazily();
    }

    if (iterator.hasNext()) {
      return iterator.next();
    } else {
      throw new NoSuchElementException();
    }
  }

  private void fetchDataLazily() throws IOException {
    final long start = System.currentTimeMillis();
    iterator = this.readable.read().iterator();
    boundedSourceReadTime += System.currentTimeMillis() - start;
  }

  final long getBoundedSourceReadTime() {
    return boundedSourceReadTime;
  }
}
