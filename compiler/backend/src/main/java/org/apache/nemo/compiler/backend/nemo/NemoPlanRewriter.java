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
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.compiler.backend.nemo.prophet.ParallelismProphet;
import org.apache.nemo.compiler.backend.nemo.prophet.Prophet;
import org.apache.nemo.compiler.backend.nemo.prophet.SkewProphet;
import org.apache.nemo.common.ir.vertex.utility.runtimepass.MessageAggregatorVertex;
import org.apache.nemo.compiler.optimizer.NemoOptimizer;
import org.apache.nemo.compiler.optimizer.pass.runtime.Message;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.plan.*;
import org.apache.nemo.runtime.master.scheduler.SimulationScheduler;
import org.apache.reef.tang.InjectionFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Rewrites the physical plan during execution, to enforce the optimizations of Nemo RunTimePasses.
 * <p>
 * A high-level flow of a rewrite is as follows:
 * Runtime - (PhysicalPlan-level info) - NemoPlanRewriter - (IRDAG-level info) - NemoOptimizer - (new IRDAG)
 * - NemoPlanRewriter - (new PhysicalPlan) - Runtime
 * <p>
 * Here, the NemoPlanRewriter acts as a translator between the Runtime that only understands PhysicalPlan-level info,
 * and the NemoOptimizer that only understands IRDAG-level info.
 * <p>
 * This decoupling between the NemoOptimizer and the Runtime lets Nemo optimization policies dynamically control
 * distributed execution behaviors, and at the same time enjoy correctness/reusability/composability properties that
 * the IRDAG abstraction provides.
 */
public final class NemoPlanRewriter implements PlanRewriter {
  private static final Logger LOG = LoggerFactory.getLogger(NemoPlanRewriter.class.getName());
  private static final String DATA_NOT_AUGMENTED = "NONE";
  private final NemoOptimizer nemoOptimizer;
  private final NemoBackend nemoBackend;
  private final Map<Integer, Map<Object, Long>> messageIdToAggregatedData;
  private CountDownLatch readyToRewriteLatch;
  private final InjectionFuture<SimulationScheduler> simulationSchedulerInjectionFuture;
  private final PhysicalPlanGenerator physicalPlanGenerator;

  private IRDAG currentIRDAG;
  private PhysicalPlan currentPhysicalPlan;

  @Inject
  public NemoPlanRewriter(final NemoOptimizer nemoOptimizer,
                          final NemoBackend nemoBackend,
                          final InjectionFuture<SimulationScheduler> simulationSchedulerInjectionFuture,
                          final PhysicalPlanGenerator physicalPlanGenerator) {
    this.nemoOptimizer = nemoOptimizer;
    this.nemoBackend = nemoBackend;
    this.simulationSchedulerInjectionFuture = simulationSchedulerInjectionFuture;
    this.physicalPlanGenerator = physicalPlanGenerator;
    this.messageIdToAggregatedData = new HashMap<>();
    this.readyToRewriteLatch = new CountDownLatch(1);
  }

  public void setCurrentIRDAG(final IRDAG currentIRDAG) {
    this.currentIRDAG = currentIRDAG;
  }

  public void setCurrentPhysicalPlan(final PhysicalPlan currentPhysicalPlan) {
    this.currentPhysicalPlan = currentPhysicalPlan;
  }
  @Override
  public PhysicalPlan rewrite(final int messageId) {
    try {
      this.readyToRewriteLatch.await();
    } catch (final InterruptedException e) {
      LOG.error("Interrupted while waiting for the rewrite latch: {}", e);
      Thread.currentThread().interrupt();
    }
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
        && e.getPropertyValue(MessageIdEdgeProperty.class).get().contains(messageId)
        && !(e.getDst() instanceof MessageAggregatorVertex))
      .collect(Collectors.toSet());
    if (examiningEdges.isEmpty()) {
      throw new IllegalArgumentException(String.valueOf(messageId));
    }

    // Optimize using the Message
    final Message message = new Message(messageId, examiningEdges, aggregatedData);
    final IRDAG newIRDAG = nemoOptimizer.optimizeAtRunTime(currentIRDAG, message);
    this.setCurrentIRDAG(newIRDAG);

    // Re-compile the IRDAG into a physical plan
    final PhysicalPlan newPhysicalPlan = nemoBackend.compile(newIRDAG);

    // Update the physical plan and return
    final List<Stage> currentStages = currentPhysicalPlan.getStageDAG().getTopologicalSort();
    final List<Stage> newStages = newPhysicalPlan.getStageDAG().getTopologicalSort();
    IntStream.range(0, currentStages.size()).forEachOrdered(i -> {
      final ExecutionPropertyMap<VertexExecutionProperty> newProperties = newStages.get(i).getExecutionProperties();
      currentStages.get(i).setExecutionProperties(newProperties);
      newProperties.get(ParallelismProperty.class).ifPresent(newParallelism -> {
        currentStages.get(i).getTaskIndices().clear();
        currentStages.get(i).getTaskIndices().addAll(IntStream.range(0, newParallelism).boxed()
          .collect(Collectors.toList()));
        IntStream.range(currentStages.get(i).getVertexIdToReadables().size(), newParallelism).forEach(newIdx ->
          currentStages.get(i).getVertexIdToReadables().add(new HashMap<>()));
      });
    });
    return currentPhysicalPlan;
  }

  /**
   * Accumulate the data needed in Plan Rewrite.
   * DATA_NOT_AUGMENTED indicates that the information need in rewrite is not stored in RunTimePassMessageEntry,
   * and we should explicitly generate it using Prophet class. In this case, the data will contain only one entry with
   * key as DATA_NOT_AUGMENTED.
   *
   * @param messageId     of the rewrite.
   * @param targetEdges   edges to change during rewrite.
   * @param data          to accumulate.
   */
  @Override
  public void accumulate(final int messageId, final Set<StageEdge> targetEdges, final Object data) {
    final Prophet prophet;
    final List<ControlMessage.RunTimePassMessageEntry> parsedData = (List<ControlMessage.RunTimePassMessageEntry>) data;
    if (!parsedData.isEmpty() && parsedData.get(0).getKey().equals(DATA_NOT_AUGMENTED)) {
      prophet = new ParallelismProphet(currentIRDAG, currentPhysicalPlan, simulationSchedulerInjectionFuture.get(),
        physicalPlanGenerator, targetEdges);
    } else {
      prophet = new SkewProphet(parsedData);
    }
    messageIdToAggregatedData.putIfAbsent(messageId, new HashMap<>());
    final Map<String, Long> aggregatedData = prophet.calculate();
    this.messageIdToAggregatedData.get(messageId).putAll(aggregatedData);
    this.readyToRewriteLatch.countDown();
  }
}
