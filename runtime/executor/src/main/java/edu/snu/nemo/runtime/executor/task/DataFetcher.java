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

import edu.snu.nemo.common.ir.vertex.IRVertex;

import java.io.IOException;

/**
 * An abstraction for fetching data from task-external sources.
 */
abstract class DataFetcher {
  private final IRVertex dataSource;
  private final VertexHarness child;
  private final boolean isToSideInput;
  private final boolean isFromSideInput;

  DataFetcher(final IRVertex dataSource,
              final VertexHarness child,
              final boolean isFromSideInput,
              final boolean isToSideInput) {
    this.dataSource = dataSource;
    this.child = child;
    this.isToSideInput = isToSideInput;
    this.isFromSideInput = isFromSideInput;
  }

  /**
   * This call can block.
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

  boolean isFromSideInput() {
    return isFromSideInput;
  }

  boolean isToSideInput() {
    return isToSideInput;
  }
}
