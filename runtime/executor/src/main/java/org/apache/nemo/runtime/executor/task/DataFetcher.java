package org.apache.nemo.runtime.executor.task;

import org.apache.nemo.common.ir.vertex.IRVertex;

import java.io.IOException;

/**
 * An abstraction for fetching data from task-external sources.
 */
abstract class DataFetcher {
  private final IRVertex dataSource;
  private final VertexHarness child;

  DataFetcher(final IRVertex dataSource,
              final VertexHarness child) {
    this.dataSource = dataSource;
    this.child = child;
  }

  /**
   * Can block until the next data element becomes available.
   * @return data element
   * @throws IOException upon I/O error
   * @throws java.util.NoSuchElementException if no more element is available
   */
  abstract Object fetchDataElement() throws IOException;

  VertexHarness getChild() {
    return child;
  }

  public IRVertex getDataSource() {
    return dataSource;
  }
}
