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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Captures the relationship between a non-source IRVertex's outputCollector, and mainTagChildren vertices.
 */
final class VertexHarness {
  private static final Logger LOG = LoggerFactory.getLogger(VertexHarness.class.getName());

  // IRVertex and transform-specific information
  private final IRVertex irVertex;
  private final OutputCollectorImpl outputCollector;
  private final Transform.Context context;
  private final List<VertexHarness> mainTagChildren;

  // These lists can be empty
  private final Map<String, VertexHarness> additionalTagOutputChildren;
  private final Map<String, String> tagToAdditionalChildrenId;
  private final List<OutputWriter> writersToMainChildrenTasks;
  private final Map<String, OutputWriter> writersToAdditionalChildrenTasks;

  VertexHarness(final IRVertex irVertex,
                final OutputCollectorImpl outputCollector,
                final List<VertexHarness> children,
                final List<Boolean> isAdditionalTagOutputs,
                final List<OutputWriter> writersToMainChildrenTasks,
                final Map<String, OutputWriter> writersToAdditionalChildrenTasks,
                final Transform.Context context) {
    this.irVertex = irVertex;
    this.outputCollector = outputCollector;
    if (children.size() != isAdditionalTagOutputs.size()) {
      throw new IllegalStateException(irVertex.toString());
    }
    final Map<String, String> taggedOutputMap = context.getTagToAdditionalChildren();
    final Map<String, VertexHarness> tagged = new HashMap<>();

    // Classify input type for intra-task children
    for (int i = 0; i < children.size(); i++) {
      final VertexHarness child = children.get(i);
      if (isAdditionalTagOutputs.get(i)) {
        taggedOutputMap.entrySet().stream()
          .filter(kv -> child.getIRVertex().getId().equals(kv.getValue()))
          .forEach(kv -> tagged.put(kv.getKey(), child));
      }
    }

    this.tagToAdditionalChildrenId = context.getTagToAdditionalChildren();
    this.additionalTagOutputChildren = tagged;
    final List<VertexHarness> mainTagChildrenTmp = new ArrayList<>(children);
    mainTagChildrenTmp.removeAll(additionalTagOutputChildren.values());
    this.mainTagChildren = mainTagChildrenTmp;
    this.writersToMainChildrenTasks = writersToMainChildrenTasks;
    this.writersToAdditionalChildrenTasks = writersToAdditionalChildrenTasks;
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
   * @return mainTagChildren harnesses.
   */
  List<VertexHarness> getMainTagChildren() {
    return mainTagChildren;
  }

  /**
   * @return map of tagged output mainTagChildren. (empty if none exists)
   */
  public Map<String, VertexHarness> getAdditionalTagOutputChildren() {
    return additionalTagOutputChildren;
  }

  /**
   * @return map of tag to additional children id.
   */
  public Map<String, String> getTagToAdditionalChildrenId() {
    return tagToAdditionalChildrenId;
  }

  /**
   * @return OutputWriters for main outputs of this irVertex. (empty if none exists)
   */
  List<OutputWriter> getWritersToMainChildrenTasks() {
    return writersToMainChildrenTasks;
  }

  /**
   * @return OutputWriters for additional tagged outputs of this irVertex. (empty if none exists)
   */
  Map<String, OutputWriter> getWritersToAdditionalChildrenTasks() {
    return writersToAdditionalChildrenTasks;
  }

  /**
   * @return context.
   */
  Transform.Context getContext() {
    return context;
  }
}
