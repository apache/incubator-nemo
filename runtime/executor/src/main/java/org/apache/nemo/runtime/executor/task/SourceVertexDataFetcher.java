package org.apache.nemo.runtime.executor.task;

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
                          final VertexHarness child) {
    super(dataSource, child);
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
