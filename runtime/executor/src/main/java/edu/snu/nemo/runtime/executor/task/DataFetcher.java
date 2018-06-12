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

import java.util.List;

abstract class DataFetcher {
  private final List<VertexHarness> consumers;

  DataFetcher(final List<VertexHarness> consumers) {
    this.consumers = consumers;
  }

  /**
   * @return null if there's no more data element.
   * @throws Exception while fetching data
   */
  abstract Object fetchDataElement() throws Exception;

  List<VertexHarness> getConsumers() {
    return consumers;
  }
}
