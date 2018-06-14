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
import edu.snu.nemo.common.ir.vertex.transform.Transform;
import edu.snu.nemo.runtime.executor.datatransfer.OutputCollectorImpl;
import edu.snu.nemo.runtime.executor.datatransfer.OutputWriter;

import java.util.ArrayList;
import java.util.List;

/**
 * Captures the relationship between a non-source IRVertex's outputCollector, and children vertices.
 */
final class VertexHarness {
  // IRVertex and transform-specific information
  private final IRVertex irVertex;
  private final OutputCollectorImpl outputCollector;
  private final Transform.Context context;

  // These lists can be empty
  private final List<VertexHarness> sideInputChildren;
  private final List<VertexHarness> nonSideInputChildren;
  private final List<OutputWriter> writersToChildrenTasks;

  VertexHarness(final IRVertex irVertex,
                final OutputCollectorImpl outputCollector,
                final List<VertexHarness> children,
                final List<Boolean> isSideInputs,
                final List<OutputWriter> writersToChildrenTasks,
                final Transform.Context context) {
    this.irVertex = irVertex;
    this.outputCollector = outputCollector;
    if (children.size() != isSideInputs.size()) {
      throw new IllegalStateException(irVertex.toString());
    }
    final List<VertexHarness> sides = new ArrayList<>();
    final List<VertexHarness> nonSides = new ArrayList<>();
    for (int i = 0; i < children.size(); i++) {
      final VertexHarness child = children.get(i);
      if (isSideInputs.get(0)) {
        sides.add(child);
      } else {
        nonSides.add(child);
      }
    }
    this.sideInputChildren = sides;
    this.nonSideInputChildren = nonSides;
    this.writersToChildrenTasks = writersToChildrenTasks;
    this.context = context;
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
   * @return list of non-sideinput children. (empty if none exists)
   */
  List<VertexHarness> getNonSideInputChildren() {
    return nonSideInputChildren;
  }

  /**
   * @return list of sideinput children. (empty if none exists)
   */
  List<VertexHarness> getSideInputChildren() {
    return sideInputChildren;
  }

  /**
   * @return OutputWriters of this irVertex. (empty if none exists)
   */
  List<OutputWriter> getWritersToChildrenTasks() {
    return writersToChildrenTasks;
  }

  /**
   * @return context.
   */
  Transform.Context getContext() {
    return context;
  }
}
