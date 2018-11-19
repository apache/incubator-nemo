/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.runtime.executor.task;

import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.runtime.executor.datatransfer.OutputWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Captures the relationship between a non-source IRVertex's outputCollector, and mainTagChildren vertices.
 */
final class VertexHarness {
  private static final Logger LOG = LoggerFactory.getLogger(VertexHarness.class.getName());

  // IRVertex and transform-specific information
  private final IRVertex irVertex;
  private final OutputCollector outputCollector;
  private final Transform.Context context;
  private final List<OutputWriter> externalOutputWriter;
  private final Map<String, List<OutputWriter>> externalAdditionalOutputWriter;

  VertexHarness(final IRVertex irVertex,
                final OutputCollector outputCollector,
                final Transform.Context context,
                final List<OutputWriter> externalOutputWriter,
                final Map<String, List<OutputWriter>> externalAdditionalOutputWriter) {
    this.irVertex = irVertex;
    this.outputCollector = outputCollector;
    this.externalOutputWriter = externalOutputWriter;
    this.externalAdditionalOutputWriter = externalAdditionalOutputWriter;
    this.context = context;
  }

  /**
   * @return irVertex of this VertexHarness.
   */
  IRVertex getIRVertex() {
    return irVertex;
  }

  /**
   * @return id of irVertex.
   */
  String getId() {
    return irVertex.getId();
  }

  /**
   * @return OutputCollector of this irVertex.
   */
  OutputCollector getOutputCollector() {
    return outputCollector;
  }

  /**
   * @return OutputWriters for main outputs of this irVertex. (empty if none exists)
   */
  List<OutputWriter> getWritersToMainChildrenTasks() {
    return externalOutputWriter;
  }

  /**
   * @return OutputWriters for additional tagged outputs of this irVertex. (empty if none exists)
   */
  Map<String, List<OutputWriter>> getWritersToAdditionalChildrenTasks() {
    return externalAdditionalOutputWriter;
  }

  /**
   * @return context.
   */
  Transform.Context getContext() {
    return context;
  }
}
