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

package org.apache.nemo.runtime.master.scheduler;

import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.message.ClientRPC;
import org.apache.nemo.runtime.common.metric.JobMetric;
import org.apache.nemo.runtime.common.metric.TaskMetric;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.common.plan.PlanRewriter;
import org.apache.nemo.runtime.common.plan.TestPlanGenerator;
import org.apache.nemo.runtime.master.BlockManagerMaster;
import org.apache.nemo.runtime.master.metric.MetricStore;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.LongStream;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests {@link SimulationScheduler}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({SchedulingConstraintRegistry.class, BlockManagerMaster.class, ClientRPC.class, BatchScheduler.class})
public final class SimulationSchedulerTest {
  private static final Logger LOG = LoggerFactory.getLogger(SimulationSchedulerTest.class.getName());
  private BatchScheduler batchScheduler;
  private SimulationScheduler scheduler;
  private static final String defaultExecutorJSONContents =
    "[{\"type\":\"Transient\",\"memory_mb\":512,\"capacity\":5},"
    + "{\"type\":\"Reserved\",\"memory_mb\":512,\"capacity\":5}]";

  // Assume no failures
  private static final int SCHEDULE_ATTEMPT_INDEX = 1;

  @Before
  public void setUp() throws Exception {
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileInstance(PlanRewriter.class, mock(PlanRewriter.class));
    injector.bindVolatileInstance(SchedulingConstraintRegistry.class, mock(SchedulingConstraintRegistry.class));
    final SchedulingPolicy schedulingPolicy = Tang.Factory.getTang().newInjector()
      .getInstance(MinOccupancyFirstSchedulingPolicy.class);
    injector.bindVolatileInstance(SchedulingPolicy.class, schedulingPolicy);
    injector.bindVolatileInstance(BlockManagerMaster.class, mock(BlockManagerMaster.class));
    injector.bindVolatileInstance(ClientRPC.class, mock(ClientRPC.class));
    injector.bindVolatileParameter(JobConf.ExecutorJSONContents.class, defaultExecutorJSONContents);
    injector.bindVolatileParameter(JobConf.ScheduleSerThread.class, 8);
    injector.bindVolatileParameter(JobConf.DAGDirectory.class, "");

    batchScheduler = mock(BatchScheduler.class);
    scheduler = injector.getInstance(SimulationScheduler.class);
  }

  @Test(timeout = 10000)
  public void testSimulation() throws Exception {
    final PhysicalPlan physicalPlan =
      TestPlanGenerator.generatePhysicalPlan(TestPlanGenerator.PlanType.TwoVerticesJoined, false);
    MetricStore.getStore().getOrCreateMetric(JobMetric.class, physicalPlan.getPlanId())
      .setStageDAG(physicalPlan.getStageDAG());
    final PhysicalPlan physicalPlan2 =
      TestPlanGenerator.generatePhysicalPlan(TestPlanGenerator.PlanType.TwoVerticesJoined, false);
    scheduler.schedulePlan(
      physicalPlan2,
      SCHEDULE_ATTEMPT_INDEX);

    final MetricStore resultingMetricStore = scheduler.collectMetricStoreAndTerminate();

    resultingMetricStore.getMetricMap(TaskMetric.class).forEach((id, taskMetric) -> {
      assertTrue(0 <= ((TaskMetric) taskMetric).getTaskDuration());
      assertTrue(100 > ((TaskMetric) taskMetric).getTaskDuration());
    });

    final LongStream l = resultingMetricStore.getMetricMap(JobMetric.class).values().stream().mapToLong(jobMetric ->
      ((JobMetric) jobMetric).getJobDuration());
    LOG.info("Simulated duration: {}ms", l.findFirst().orElse(0));

    LOG.info("Wait for plan termination after sending stage completion events");
    assertTrue(scheduler.getPlanStateManager().isPlanDone());
  }
}
