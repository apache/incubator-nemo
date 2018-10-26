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
import org.apache.nemo.common.ir.vertex.IRVertex;

import java.io.IOException;

/**
 * An abstraction for fetching data from task-external sources.
 */
abstract class DataFetcher {
  private final IRVertex dataSource;
  private final OutputCollector outputCollector;

  DataFetcher(final IRVertex dataSource,
              final OutputCollector outputCollector) {
    this.dataSource = dataSource;
    this.outputCollector = outputCollector;
  }

  /**
   * Can block until the next data element becomes available.
   * @return data element
   * @throws IOException upon I/O error
   * @throws java.util.NoSuchElementException if no more element is available
   */
  abstract Object fetchDataElement() throws IOException;

  OutputCollector getOutputCollector() {
    return outputCollector;
  }

  public IRVertex getDataSource() {
    return dataSource;
  }
}
