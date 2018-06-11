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
import edu.snu.nemo.runtime.executor.datatransfer.OutputWriter;

import java.util.ArrayList;
import java.util.List;

/**
 * For a non-root vertex of a task.
 */
final class NonRootDataHandler extends DataHandler {
  // This list is not empty if the IRVertex has a child IRVertex belonging to a different task.
  private final List<OutputWriter> writersToChildrenTasks;

  /**
   * @param irVertex vertex.
   * @param outputCollector outputCollector.
   * @param children handlers.
   */
  NonRootDataHandler(final IRVertex irVertex,
                     final OutputCollectorImpl outputCollector,
                     final List<DataHandler> children) {
    this(irVertex, outputCollector, children, new ArrayList<>(0));
  }

  /**
   * @param irVertex vertex.
   * @param outputCollector outputCollector.
   * @param children handlers.
   * @param writersToChildrenTasks writers.
   */
  NonRootDataHandler(final IRVertex irVertex,
                     final OutputCollectorImpl outputCollector,
                     final List<DataHandler> children,
                     final List<OutputWriter> writersToChildrenTasks) {
    super(irVertex, outputCollector, children);
    this.writersToChildrenTasks = writersToChildrenTasks;
  }

  /**
   * @return OutputWriters of this irVertex. (empty if none exists)
   */
  List<OutputWriter> getWritersToChildrenTasks() {
    return writersToChildrenTasks;
  }
}
