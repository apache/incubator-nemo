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
package org.apache.nemo.compiler.backend.nemo.prophet;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.PartitionerProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.runtime.common.metric.JobMetric;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.common.plan.PhysicalPlanGenerator;
import org.apache.nemo.runtime.common.plan.Stage;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.runtime.master.metric.MetricStore;
import org.apache.nemo.runtime.master.scheduler.SimulationScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A prophet for Parallelism.
 */
public final class ParallelismProphet implements Prophet<String, Long> {
  private static final Logger LOG = LoggerFactory.getLogger(ParallelismProphet.class.getName());
  private final SimulationScheduler simulationScheduler;
  private final PhysicalPlanGenerator physicalPlanGenerator;
  private final IRDAG currentIRDAG;
  private final PhysicalPlan currentPhysicalPlan;
  private final Set<StageEdge> edgesToOptimize;
  private final int partitionerProperty;

  /**
   * Default constructor for ParallelismProphet.
   * @param irdag                   current IRDAG
   * @param physicalPlan            current PhysicalPlan
   * @param simulationScheduler     SimulationScheduler to launch
   * @param physicalPlanGenerator   PhysicalPlanGenerator to make physical plan which will be launched by
   *                                simulation scheduler
   * @param edgesToOptimize         edges to optimize at runtime pass
   */
  public ParallelismProphet(final IRDAG irdag, final PhysicalPlan physicalPlan,
                            final SimulationScheduler simulationScheduler,
                            final PhysicalPlanGenerator physicalPlanGenerator,
                            final Set<StageEdge> edgesToOptimize) {
    this.currentIRDAG = irdag;
    this.currentPhysicalPlan = physicalPlan;
    this.simulationScheduler = simulationScheduler;
    this.physicalPlanGenerator = physicalPlanGenerator;
    this.edgesToOptimize = edgesToOptimize;
    this.partitionerProperty = calculatePartitionerProperty(edgesToOptimize);
  }

  /**
   * Launch SimulationScheduler and find out the optimal parallelism.
   * For now, the number of candidate parallelisms is seven, so we iterate seven times.
   * In each iteration index i, the candidate parallelism is calculated by dividing the i-th power of two from
   * partitonerProperty (which is guaranteed to be one of 1024, 2048, 4096. For more information, please refer to
   * SamplingTaskSizingPass). This approach is taken to guarantee the equal length of each partition, which will be
   * updated in DynamicTaskSizingRuntimePass.
   *
   * @return  Map of one element, with key "opt.parallelism".
   */
  @Override
  public Map<String, Long> calculate() {
    final Map<String, Long> result = new HashMap<>();
    final List<PhysicalPlan> listOfPhysicalPlans = new ArrayList<>(); // when to update here?
    for (int i = 0; i < 7; i++) {
      final int parallelism = (int) (partitionerProperty / Math.pow(2, i));
      PhysicalPlan newPlan = makePhysicalPlanForSimulation(parallelism, edgesToOptimize, currentIRDAG);
      listOfPhysicalPlans.add(newPlan);
    }
    final List<Pair<Integer, Long>> listOfParallelismToDurationPair = listOfPhysicalPlans.stream()
      .map(this::launchSimulationForPlan)
      .filter(pair -> pair.right() > 0.5)
      .collect(Collectors.toList());
    final Pair<Integer, Long> pairWithMinDuration =
      Collections.min(listOfParallelismToDurationPair, Comparator.comparing(p -> p.right()));
    result.put("opt.parallelism", pairWithMinDuration.left().longValue());
    return result;
  }

  /**
   * Simulate the given physical plan. The simulationScheduler schedules plan only once, to reduce further overhead
   * from simulation.
   * @param physicalPlan      physical plan(with only one stage) to simulate
   * @return                  Pair of Integer and Long. Integer value indicates the simulated parallelism, and
   *                          Long value is simulated job(=stage) duration.
   */
  private synchronized Pair<Integer, Long> launchSimulationForPlan(final PhysicalPlan physicalPlan) {
    this.simulationScheduler.schedulePlan(physicalPlan, 1);
    final MetricStore resultingMetricStore = this.simulationScheduler.collectMetricStore();
    final List<Pair<Integer, Long>> taskSizeRatioToDuration = new ArrayList<>();
    resultingMetricStore.getMetricMap(JobMetric.class).values().forEach(jobMetric -> {
      final int taskSizeRatio = Integer.parseInt(((JobMetric) jobMetric).getId().split("-")[1]);
      taskSizeRatioToDuration.add(Pair.of(taskSizeRatio, ((JobMetric) jobMetric).getJobDuration()));
    });
    return Collections.min(taskSizeRatioToDuration, Comparator.comparing(Pair::right));
  }

  /**
   * Calculate the partitioner property of the target stage.
   * @param edges Edges to optimize(i.e. edges pointing to the target stage vertices)
   *              Edges are considered to have all same partitioner property value.
   */
  private int calculatePartitionerProperty(final Set<StageEdge> edges) {
    return edges.iterator().next().getPropertyValue(PartitionerProperty.class).get().right();
  }

  /**
   * Make Physical plan which is to be launched in Simulation Scheduler.
   * @param parallelism   parallelism to set in new physical plan
   * @param edges         stageEdges to find stages which will be included in new physical plan
   * @param currentDag    current dag to reference
   * @return              Physical plan with consists of selected vertices with given parallelism
   */
  private PhysicalPlan makePhysicalPlanForSimulation(final int parallelism,
                                                     final Set<StageEdge> edges,
                                                     final IRDAG currentDag) {
    Set<IRVertex> verticesToChangeParallelism = edges.stream()
      .map(edge -> edge.getDst().getIRDAG().getVertices())
      .flatMap(Collection::stream).collect(Collectors.toSet());
    final DAGBuilder<IRVertex, IREdge> dagBuilder = new DAGBuilder<>();

    // make dag of only one stage (targetStage)
    currentDag.topologicalDo(v -> {
      if (verticesToChangeParallelism.contains(v)) {
        v.setProperty(ParallelismProperty.of(parallelism));
        dagBuilder.addVertex(v);
        for (IREdge edge : currentDag.getIncomingEdgesOf(v)) {
          if (verticesToChangeParallelism.contains(edge.getSrc())) {
            dagBuilder.connectVertices(edge);
          }
        }
      }
    });
    final IRDAG newDag = new IRDAG(dagBuilder.buildWithoutSourceSinkCheck());
    final DAG<Stage, StageEdge> stageDag = physicalPlanGenerator.stagePartitionIrDAG(newDag);
    return new PhysicalPlan(currentPhysicalPlan.getPlanId().concat("-" + parallelism), stageDag);
  }
}
