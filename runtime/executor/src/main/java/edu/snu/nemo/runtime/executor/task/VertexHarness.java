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
import edu.snu.nemo.runtime.executor.datatransfer.InputReader;
import edu.snu.nemo.runtime.executor.datatransfer.OutputCollectorImpl;
import edu.snu.nemo.runtime.executor.datatransfer.OutputWriter;

import java.util.List;

/**
 * Captures the relationship between an IRVertex's outputCollector, and children vertices.
 */
abstract class VertexHarness {
  // IRVertex and its corresponding output collector.
  private final IRVertex irVertex;
  private final OutputCollectorImpl outputCollector;

  // These lists can be empty
  private final List<VertexHarness> children;
  private final List<InputReader> readersForParentTasks;
  private final List<OutputWriter> writersToChildrenTasks;

  VertexHarness(final IRVertex irVertex,
                final OutputCollectorImpl outputCollector,
                final List<VertexHarness> children,
                final List<InputReader> readersForParentTasks,
                final List<OutputWriter> writersToChildrenTasks) {
    this.irVertex = irVertex;
    this.outputCollector = outputCollector;
    this.children = children;
    this.readersForParentTasks = readersForParentTasks;
    this.writersToChildrenTasks = writersToChildrenTasks;
  }

  /**
   * @return irVertex of this VertexHarness.
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
   * @return list of children. (empty if none exists)
   */
  List<VertexHarness> getChildren() {
    return children;
  }

  /**
   * @return InputReaders of this irVertex. (empty if none exists)
   */
  List<InputReader> getReadersForParentTasks() {
    return readersForParentTasks;
  }

  /**
   * @return OutputWriters of this irVertex. (empty if none exists)
   */
  List<OutputWriter> getWritersToChildrenTasks() {
    return writersToChildrenTasks;
  }
}
