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
package org.apache.nemo.compiler.backend.nemo;

import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.MessageIdEdgeProperty;
import org.apache.nemo.common.ir.executionproperty.ExecutionPropertyMap;
import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;
import org.apache.nemo.common.ir.vertex.utility.MessageAggregatorVertex;
import org.apache.nemo.compiler.optimizer.NemoOptimizer;
import org.apache.nemo.compiler.optimizer.pass.runtime.Message;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.common.plan.PlanRewriter;
import org.apache.nemo.runtime.common.plan.Stage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Rewrites the physical plan during execution, to enforce the optimizations of Nemo RunTimePasses.
 *
 * A high-level flow of a rewrite is as follows:
 * Runtime - (PhysicalPlan-level info) - NemoPlanRewriter - (IRDAG-level info) - NemoOptimizer - (new IRDAG)
 * - NemoPlanRewriter - (new PhysicalPlan) - Runtime
 *
 * Here, the NemoPlanRewriter acts as a translator between the Runtime that only understands PhysicalPlan-level info,
 * and the NemoOptimizer that only understands IRDAG-level info.
 *
 * This decoupling between the NemoOptimizer and the Runtime lets Nemo optimization policies dynamically control
 * distributed execution behaviors, and at the same time enjoy correctness/reusability/composability properties that
 * the IRDAG abstraction provides.
 */
public final class NemoPlanRewriter implements PlanRewriter {
  private static final Logger LOG = LoggerFactory.getLogger(NemoPlanRewriter.class.getName());

  private final NemoOptimizer nemoOptimizer;
  private final NemoBackend nemoBackend;
  private final Map<Integer, Map<Object, Long>> messageIdToAggregatedData;

  private IRDAG currentIRDAG;

  @Inject
  public NemoPlanRewriter(final NemoOptimizer nemoOptimizer,
                          final NemoBackend nemoBackend) {
    this.nemoOptimizer = nemoOptimizer;
    this.nemoBackend = nemoBackend;
    this.messageIdToAggregatedData = new HashMap<>();
  }

  public void setIRDAG(final IRDAG irdag) {
    this.currentIRDAG = irdag;
  }

  @Override
  public PhysicalPlan rewrite(final PhysicalPlan currentPhysicalPlan, final int messageId) {
    if (currentIRDAG == null) {
      throw new IllegalStateException();
    }
    final Map<Object, Long> aggregatedData = messageIdToAggregatedData.remove(messageId); // remove for GC
    if (aggregatedData == null) {
      throw new IllegalStateException();
    }

    // Find IREdges using the messageId
    final Set<IREdge> examiningEdges = currentIRDAG
      .getVertices()
      .stream()
      .flatMap(v -> currentIRDAG.getIncomingEdgesOf(v).stream())
      .filter(e -> e.getPropertyValue(MessageIdEdgeProperty.class).isPresent()
        && e.getPropertyValue(MessageIdEdgeProperty.class).get() == messageId
        && !(e.getDst() instanceof MessageAggregatorVertex))
      .collect(Collectors.toSet());
    if (examiningEdges.isEmpty()) {
      throw new IllegalArgumentException(String.valueOf(messageId));
    }

    // Optimize using the Message
    final Message message = new Message(messageId, examiningEdges, aggregatedData);
    final IRDAG newIRDAG = nemoOptimizer.optimizeAtRunTime(currentIRDAG, message);

    // Re-compile the IRDAG into a physical plan
    final PhysicalPlan newPhysicalPlan = nemoBackend.compile(newIRDAG);

    // Update the physical plan and return
    final List<Stage> currentStages = currentPhysicalPlan.getStageDAG().getTopologicalSort();
    final List<Stage> newStages = newPhysicalPlan.getStageDAG().getTopologicalSort();
    for (int i = 0; i < currentStages.size(); i++) {
      final ExecutionPropertyMap<VertexExecutionProperty> newProperties = newStages.get(i).getExecutionProperties();
      currentStages.get(i).setExecutionProperties(newProperties);
    }
    return currentPhysicalPlan;
  }

  @Override
  public void accumulate(final int messageId, final Object data) {
    messageIdToAggregatedData.putIfAbsent(messageId, new HashMap<>());
    final Map<Object, Long> aggregatedData = messageIdToAggregatedData.get(messageId);
    final List<ControlMessage.RunTimePassMessageEntry> messageEntries =
      (List<ControlMessage.RunTimePassMessageEntry>) data;
    messageEntries.forEach(entry -> {
      final Object key = entry.getKey();
      final long partitionSize = entry.getValue();
      if (aggregatedData.containsKey(key)) {
        aggregatedData.compute(key, (originalKey, originalValue) -> originalValue + partitionSize);
      } else {
        aggregatedData.put(key, partitionSize);
      }
    });
  }
}
