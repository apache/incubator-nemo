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

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.eventhandler.PubSubEventHandlerWrapper;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageSender;
import org.apache.nemo.runtime.common.plan.*;
import org.apache.nemo.runtime.master.BlockManagerMaster;
import org.apache.nemo.runtime.master.PlanStateManager;
import org.apache.nemo.runtime.master.metric.MetricMessageHandler;
import org.apache.nemo.runtime.master.resource.DefaultExecutorRepresenter;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.nemo.common.ir.executionproperty.ResourceSpecification;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests {@link BatchScheduler}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({BlockManagerMaster.class, PubSubEventHandlerWrapper.class})
public final class BatchSchedulerTest {
  private static final Logger LOG = LoggerFactory.getLogger(BatchSchedulerTest.class.getName());
  private BatchScheduler scheduler;
  private PlanStateManager planStateManager;
  private ExecutorRegistry executorRegistry;
  private final MessageSender<ControlMessage.Message> mockMsgSender = mock(MessageSender.class);

  private static final int EXECUTOR_CAPACITY = 20;

  // Assume no failures
  private static final int SCHEDULE_ATTEMPT_INDEX = 1;

  @Before
  public void setUp() throws Exception {
    final Injector injector = Tang.Factory.getTang().newInjector();
    final PlanRewriter planRewriter = mock(PlanRewriter.class);
    injector.bindVolatileInstance(PlanRewriter.class, planRewriter);
    injector.bindVolatileParameter(JobConf.DAGDirectory.class, "");

    executorRegistry = injector.getInstance(ExecutorRegistry.class);
    injector.bindVolatileInstance(BlockManagerMaster.class, mock(BlockManagerMaster.class));
    injector.bindVolatileInstance(PubSubEventHandlerWrapper.class, mock(PubSubEventHandlerWrapper.class));
    injector.bindVolatileInstance(MetricMessageHandler.class, mock(MetricMessageHandler.class));
    planStateManager = injector.getInstance(PlanStateManager.class);
    scheduler = injector.getInstance(BatchScheduler.class);

    final ActiveContext activeContext = mock(ActiveContext.class);
    Mockito.doThrow(new RuntimeException()).when(activeContext).close();

    final ExecutorService serializationExecutorService = Executors.newSingleThreadExecutor();
    final ResourceSpecification computeSpec =
      new ResourceSpecification(ResourcePriorityProperty.COMPUTE, EXECUTOR_CAPACITY, 0);
    final Function<String, ExecutorRepresenter> computeSpecExecutorRepresenterGenerator = executorId ->
      new DefaultExecutorRepresenter(executorId, computeSpec, mockMsgSender, activeContext, serializationExecutorService,
        executorId);
    final ExecutorRepresenter a3 = computeSpecExecutorRepresenterGenerator.apply("a3");
    final ExecutorRepresenter a2 = computeSpecExecutorRepresenterGenerator.apply("a2");
    final ExecutorRepresenter a1 = computeSpecExecutorRepresenterGenerator.apply("a1");

    final ResourceSpecification storageSpec =
      new ResourceSpecification(ResourcePriorityProperty.TRANSIENT, EXECUTOR_CAPACITY, 0);
    final Function<String, ExecutorRepresenter> storageSpecExecutorRepresenterGenerator = executorId ->
      new DefaultExecutorRepresenter(executorId, storageSpec, mockMsgSender, activeContext, serializationExecutorService,
        executorId);
    final ExecutorRepresenter b2 = storageSpecExecutorRepresenterGenerator.apply("b2");
    final ExecutorRepresenter b1 = storageSpecExecutorRepresenterGenerator.apply("b1");

    // Add compute nodes
    scheduler.onExecutorAdded(a1);
    scheduler.onExecutorAdded(a2);
    scheduler.onExecutorAdded(a3);

    // Add storage nodes
    scheduler.onExecutorAdded(b1);
    scheduler.onExecutorAdded(b2);
  }

  /**
   * This method builds a physical DAG starting from an IR DAG and submits it to {@link BatchScheduler}.
   * Task state changes are explicitly submitted to scheduler instead of executor messages.
   *
   * @throws Exception exception on the way.
   */
  @Test(timeout = 10000)
  public void testPull() throws Exception {
    scheduleAndCheckPlanTermination(
      TestPlanGenerator.generatePhysicalPlan(TestPlanGenerator.PlanType.TwoVerticesJoined, false));
  }

  /**
   * This method builds a physical DAG starting from an IR DAG and submits it to {@link BatchScheduler}.
   * Task state changes are explicitly submitted to scheduler instead of executor messages.
   *
   * @throws Exception exception on the way.
   */
  @Test(timeout = 10000)
  public void testPush() throws Exception {
    scheduleAndCheckPlanTermination(
      TestPlanGenerator.generatePhysicalPlan(TestPlanGenerator.PlanType.TwoVerticesJoined, true));
  }

  private void scheduleAndCheckPlanTermination(final PhysicalPlan plan) throws InjectionException {
    scheduler.schedulePlan(plan, 1);

    // For each ScheduleGroup, test if the tasks of the next ScheduleGroup are scheduled
    // after the stages of each ScheduleGroup are made "complete".
    for (int i = 0; i < getNumScheduleGroups(plan.getStageDAG()); i++) {
      final int scheduleGroupIdx = i;
      final List<Stage> stages = filterStagesWithAScheduleGroup(plan.getStageDAG(), scheduleGroupIdx);

      LOG.debug("Checking that all stages of ScheduleGroup {} enter the executing state", scheduleGroupIdx);
      stages.forEach(stage -> {
        SchedulerTestUtil.completeStage(
          planStateManager, scheduler, executorRegistry, stage, SCHEDULE_ATTEMPT_INDEX);
      });
    }

    LOG.debug("Waiting for plan termination after sending stage completion events");
    while (!planStateManager.isPlanDone()) {
    }
    assertTrue(planStateManager.isPlanDone());
  }

  private List<Stage> filterStagesWithAScheduleGroup(
    final DAG<Stage, StageEdge> physicalDAG, final int scheduleGroup) {
    final Set<Stage> stageSet = new HashSet<>(physicalDAG.filterVertices(
      stage -> stage.getScheduleGroup() == scheduleGroup));

    // Return the filtered vertices as a sorted list
    final List<Stage> sortedStages = new ArrayList<>(stageSet.size());
    physicalDAG.topologicalDo(stage -> {
      if (stageSet.contains(stage)) {
        sortedStages.add(stage);
      }
    });
    return sortedStages;
  }

  private int getNumScheduleGroups(final DAG<Stage, StageEdge> physicalDAG) {
    final Set<Integer> scheduleGroupSet = new HashSet<>();
    physicalDAG.getVertices().forEach(stage -> scheduleGroupSet.add(stage.getScheduleGroup()));
    return scheduleGroupSet.size();
  }
}
