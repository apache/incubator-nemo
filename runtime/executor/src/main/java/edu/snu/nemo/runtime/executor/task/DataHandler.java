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
import edu.snu.nemo.runtime.executor.datatransfer.OutputCollectorImpl;

import java.util.List;

/**
 * Captures the relationship between an IRVertex's outputCollector, and children vertices.
 */
abstract class DataHandler {
  // IRVertex and its corresponding output collector.
  private final IRVertex irVertex;
  private final OutputCollectorImpl outputCollector;

  // Children handlers. (can be empty)
  private List<DataHandler> children;

  DataHandler(final IRVertex irVertex,
              final OutputCollectorImpl outputCollector,
              final List<DataHandler> children) {
    this.irVertex = irVertex;
    this.outputCollector = outputCollector;
    this.children = children;
  }

  /**
   * @return irVertex of this DataHandler.
   */
  IRVertex getIRVertex() {
    return irVertex;
  }

  /**
   * @return OutputCollector of this irVertex.
   */
  OutputCollectorImpl getOutputCollector() {
    return outputCollector;
  }

  /**
   * @return list of children.
   */
  List<DataHandler> getChildren() {
    return children;
  }
}
