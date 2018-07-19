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
package edu.snu.nemo.runtime.master.scheduler;

import edu.snu.nemo.common.eventhandler.PubSubEventHandlerWrapper;
import edu.snu.nemo.common.ir.vertex.executionproperty.ExecutorPlacementProperty;
import edu.snu.nemo.conf.JobConf;
import edu.snu.nemo.runtime.common.comm.ControlMessage;
import edu.snu.nemo.runtime.common.message.MessageSender;
import edu.snu.nemo.runtime.common.plan.PhysicalPlan;
import edu.snu.nemo.runtime.common.plan.Stage;
import edu.snu.nemo.runtime.common.plan.StageEdge;
import edu.snu.nemo.runtime.master.JobStateManager;
import edu.snu.nemo.runtime.master.MetricMessageHandler;
import edu.snu.nemo.runtime.master.BlockManagerMaster;
import edu.snu.nemo.runtime.master.eventhandler.UpdatePhysicalPlanEventHandler;
import edu.snu.nemo.runtime.master.resource.ContainerManager;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
import edu.snu.nemo.runtime.master.resource.ResourceSpecification;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.runtime.common.plan.TestPlanGenerator;
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

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests {@link BatchSingleJobScheduler}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ContainerManager.class, BlockManagerMaster.class,
    PubSubEventHandlerWrapper.class, UpdatePhysicalPlanEventHandler.class, MetricMessageHandler.class})
public final class BatchSingleJobSchedulerTest {
  private static final Logger LOG = LoggerFactory.getLogger(BatchSingleJobSchedulerTest.class.getName());
  private Scheduler scheduler;
  private ExecutorRegistry executorRegistry;
  private final MetricMessageHandler metricMessageHandler = mock(MetricMessageHandler.class);
  private final MessageSender<ControlMessage.Message> mockMsgSender = mock(MessageSender.class);

  private static final int EXECUTOR_CAPACITY = 20;

  // Assume no failures
  private static final int SCHEDULE_ATTEMPT_INDEX = 1;

  @Before
  public void setUp() throws Exception {
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(JobConf.DAGDirectory.class, "");

    executorRegistry = injector.getInstance(ExecutorRegistry.class);
    injector.bindVolatileInstance(BlockManagerMaster.class, mock(BlockManagerMaster.class));
    injector.bindVolatileInstance(PubSubEventHandlerWrapper.class, mock(PubSubEventHandlerWrapper.class));
    injector.bindVolatileInstance(UpdatePhysicalPlanEventHandler.class, mock(UpdatePhysicalPlanEventHandler.class));
    scheduler = injector.getInstance(BatchSingleJobScheduler.class);

    final ActiveContext activeContext = mock(ActiveContext.class);
    Mockito.doThrow(new RuntimeException()).when(activeContext).close();

    final ExecutorService serializationExecutorService = Executors.newSingleThreadExecutor();
    final ResourceSpecification computeSpec =
        new ResourceSpecification(ExecutorPlacementProperty.COMPUTE, EXECUTOR_CAPACITY, 0);
    final Function<String, ExecutorRepresenter> computeSpecExecutorRepresenterGenerator = executorId ->
        new ExecutorRepresenter(executorId, computeSpec, mockMsgSender, activeContext, serializationExecutorService,
            executorId);
    final ExecutorRepresenter a3 = computeSpecExecutorRepresenterGenerator.apply("a3");
    final ExecutorRepresenter a2 = computeSpecExecutorRepresenterGenerator.apply("a2");
    final ExecutorRepresenter a1 = computeSpecExecutorRepresenterGenerator.apply("a1");

    final ResourceSpecification storageSpec =
        new ResourceSpecification(ExecutorPlacementProperty.TRANSIENT, EXECUTOR_CAPACITY, 0);
    final Function<String, ExecutorRepresenter> storageSpecExecutorRepresenterGenerator = executorId ->
        new ExecutorRepresenter(executorId, storageSpec, mockMsgSender, activeContext, serializationExecutorService,
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
   * This method builds a physical DAG starting from an IR DAG and submits it to {@link BatchSingleJobScheduler}.
   * Task state changes are explicitly submitted to scheduler instead of executor messages.
   */
  @Test(timeout=10000)
  public void testPull() throws Exception {
    scheduleAndCheckJobTermination(
        TestPlanGenerator.generatePhysicalPlan(TestPlanGenerator.PlanType.TwoVerticesJoined, false));
  }

  /**
   * This method builds a physical DAG starting from an IR DAG and submits it to {@link BatchSingleJobScheduler}.
   * Task state changes are explicitly submitted to scheduler instead of executor messages.
   */
  @Test(timeout=10000)
  public void testPush() throws Exception {
    scheduleAndCheckJobTermination(
        TestPlanGenerator.generatePhysicalPlan(TestPlanGenerator.PlanType.TwoVerticesJoined, true));
  }

  private void scheduleAndCheckJobTermination(final PhysicalPlan plan) throws InjectionException {
    final JobStateManager jobStateManager = new JobStateManager(plan, metricMessageHandler, 1);
    scheduler.scheduleJob(plan, jobStateManager);

    // For each ScheduleGroup, test if the tasks of the next ScheduleGroup are scheduled
    // after the stages of each ScheduleGroup are made "complete".
    for (int i = 0; i < getNumScheduleGroups(plan.getStageDAG()); i++) {
      final int scheduleGroupIdx = i;
      final List<Stage> stages = filterStagesWithAScheduleGroup(plan.getStageDAG(), scheduleGroupIdx);

      LOG.debug("Checking that all stages of ScheduleGroup {} enter the executing state", scheduleGroupIdx);
      stages.forEach(stage -> {
        SchedulerTestUtil.completeStage(
            jobStateManager, scheduler, executorRegistry, stage, SCHEDULE_ATTEMPT_INDEX);
      });
    }

    LOG.debug("Waiting for job termination after sending stage completion events");
    while (!jobStateManager.isJobDone()) {
    }
    assertTrue(jobStateManager.isJobDone());
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
