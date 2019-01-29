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
import org.apache.nemo.compiler.optimizer.NemoOptimizer;
import org.apache.nemo.compiler.optimizer.pass.runtime.Message;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.common.plan.PlanRewriter;

import javax.inject.Inject;
import java.util.*;

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
public class NemoPlanRewriter implements PlanRewriter {
  private final NemoOptimizer nemoOptimizer;
  private final NemoBackend nemoBackend;
  private final Map<String, Map<Object, Long>> messageIdToAggregatedData;

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
  public PhysicalPlan rewrite(final String messageId) {
    if (currentIRDAG == null) {
      throw new IllegalStateException();
    }

    final Map<Object, Long> aggregatedData = messageIdToAggregatedData.remove(messageId); // remove for GC
    if (aggregatedData == null) {
      throw new IllegalStateException();
    }

    final Set<IREdge> examiningEdges = currentIRDAG.topologicalDo(); // find edges using the messageId (exec props)
    final Message message = new Message(messageId, examiningEdges, aggregatedData);
    final IRDAG newIRDAG = nemoOptimizer.optimizeAtRunTime(currentIRDAG, message);
    return nemoBackend.compile(newIRDAG); // must be compatible with the existing plan
  }

  @Override
  public void accumulate(final String messageId, final Object data) {
    messageIdToAggregatedData.putIfAbsent(messageId, new HashMap<>());
    final Map<Object, Long> aggregatedData = messageIdToAggregatedData.get(messageId);
    final List<ControlMessage.Entry> messageEntries = (List<ControlMessage.Entry>) data;
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
