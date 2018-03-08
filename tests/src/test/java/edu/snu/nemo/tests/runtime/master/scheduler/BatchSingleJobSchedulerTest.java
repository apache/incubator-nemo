/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.nemo.tests.runtime.master.scheduler;

import edu.snu.nemo.common.coder.Coder;
import edu.snu.nemo.common.eventhandler.PubSubEventHandlerWrapper;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.OperatorVertex;
import edu.snu.nemo.common.ir.vertex.executionproperty.ExecutorPlacementProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import edu.snu.nemo.compiler.frontend.beam.transform.DoTransform;
import edu.snu.nemo.common.ir.vertex.transform.Transform;
import edu.snu.nemo.common.ir.executionproperty.ExecutionProperty;
import edu.snu.nemo.compiler.optimizer.CompiletimeOptimizer;
import edu.snu.nemo.compiler.optimizer.examples.EmptyComponents;
import edu.snu.nemo.conf.JobConf;
import edu.snu.nemo.runtime.master.scheduler.ExecutorRegistry;
import edu.snu.nemo.tests.runtime.RuntimeTestUtil;
import edu.snu.nemo.runtime.common.comm.ControlMessage;
import edu.snu.nemo.runtime.common.message.MessageSender;
import edu.snu.nemo.runtime.common.plan.physical.*;
import edu.snu.nemo.runtime.common.state.StageState;
import edu.snu.nemo.runtime.master.JobStateManager;
import edu.snu.nemo.runtime.master.MetricMessageHandler;
import edu.snu.nemo.runtime.master.BlockManagerMaster;
import edu.snu.nemo.runtime.master.eventhandler.UpdatePhysicalPlanEventHandler;
import edu.snu.nemo.runtime.master.resource.ContainerManager;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
import edu.snu.nemo.runtime.master.resource.ResourceSpecification;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.runtime.master.scheduler.*;
import edu.snu.nemo.tests.compiler.optimizer.policy.TestPolicy;
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
  private DAGBuilder<IRVertex, IREdge> irDAGBuilder;
  private Scheduler scheduler;
  private SchedulingPolicy schedulingPolicy;
  private SchedulerRunner schedulerRunner;
  private ExecutorRegistry executorRegistry;
  private MetricMessageHandler metricMessageHandler;
  private PendingTaskGroupQueue pendingTaskGroupQueue;
  private PubSubEventHandlerWrapper pubSubEventHandler;
  private UpdatePhysicalPlanEventHandler updatePhysicalPlanEventHandler;
  private BlockManagerMaster blockManagerMaster = mock(BlockManagerMaster.class);
  private final MessageSender<ControlMessage.Message> mockMsgSender = mock(MessageSender.class);
  private PhysicalPlanGenerator physicalPlanGenerator;

  private static final int TEST_TIMEOUT_MS = 500;

  private static final int EXECUTOR_CAPACITY = 20;

  // This schedule index will make sure that task group events are not ignored
  private static final int MAGIC_SCHEDULE_ATTEMPT_INDEX = Integer.MAX_VALUE;

  @Before
  public void setUp() throws Exception {
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(JobConf.DAGDirectory.class, "");

    irDAGBuilder = initializeDAGBuilder();
    executorRegistry = injector.getInstance(ExecutorRegistry.class);
    metricMessageHandler = mock(MetricMessageHandler.class);
    pendingTaskGroupQueue = new SingleJobTaskGroupQueue();
    schedulingPolicy = new RoundRobinSchedulingPolicy(executorRegistry, TEST_TIMEOUT_MS);
    schedulerRunner = new SchedulerRunner(schedulingPolicy, pendingTaskGroupQueue);
    pubSubEventHandler = mock(PubSubEventHandlerWrapper.class);
    updatePhysicalPlanEventHandler = mock(UpdatePhysicalPlanEventHandler.class);
    scheduler =
        new BatchSingleJobScheduler(schedulingPolicy, schedulerRunner, pendingTaskGroupQueue,
            blockManagerMaster, pubSubEventHandler, updatePhysicalPlanEventHandler);

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

    physicalPlanGenerator = injector.getInstance(PhysicalPlanGenerator.class);
  }

  /**
   * This method builds a physical DAG starting from an IR DAG and submits it to {@link BatchSingleJobScheduler}.
   * TaskGroup state changes are explicitly submitted to scheduler instead of executor messages.
   */
  @Test(timeout=10000)
  public void testPull() throws Exception {
    final DAG<IRVertex, IREdge> pullIRDAG = CompiletimeOptimizer.optimize(irDAGBuilder.buildWithoutSourceSinkCheck(),
        new TestPolicy(), "");
    scheduleAndCheckJobTermination(pullIRDAG);
  }

  /**
   * This method builds a physical DAG starting from an IR DAG and submits it to {@link BatchSingleJobScheduler}.
   * TaskGroup state changes are explicitly submitted to scheduler instead of executor messages.
   */
  @Test(timeout=10000)
  public void testPush() throws Exception {
    final DAG<IRVertex, IREdge> pushIRDAG = CompiletimeOptimizer.optimize(irDAGBuilder.buildWithoutSourceSinkCheck(),
        new TestPolicy(true), "");
    scheduleAndCheckJobTermination(pushIRDAG);
  }

  private DAGBuilder<IRVertex, IREdge> initializeDAGBuilder() {
    final DAGBuilder<IRVertex, IREdge> dagBuilder = new DAGBuilder<>();

    final Transform t = new EmptyComponents.EmptyTransform("empty");
    final IRVertex v1 = new OperatorVertex(t);
    v1.setProperty(ParallelismProperty.of(1));
    v1.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.COMPUTE));
    dagBuilder.addVertex(v1);

    final IRVertex v2 = new OperatorVertex(t);
    v2.setProperty(ParallelismProperty.of(2));
    v2.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.COMPUTE));
    dagBuilder.addVertex(v2);

    final IRVertex v3 = new OperatorVertex(t);
    v3.setProperty(ParallelismProperty.of(3));
    v3.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.COMPUTE));
    dagBuilder.addVertex(v3);

    final IRVertex v4 = new OperatorVertex(t);
    v4.setProperty(ParallelismProperty.of(4));
    v4.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.COMPUTE));
    dagBuilder.addVertex(v4);

    final IRVertex v5 = new OperatorVertex(new DoTransform(null, null));
    v5.setProperty(ParallelismProperty.of(5));
    v5.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.TRANSIENT));
    dagBuilder.addVertex(v5);

    final IREdge e1 = new IREdge(DataCommunicationPatternProperty.Value.Shuffle, v1, v2, Coder.DUMMY_CODER);
    dagBuilder.connectVertices(e1);

    final IREdge e2 = new IREdge(DataCommunicationPatternProperty.Value.Shuffle, v3, v2, Coder.DUMMY_CODER);
    dagBuilder.connectVertices(e2);

    final IREdge e4 = new IREdge(DataCommunicationPatternProperty.Value.Shuffle, v2, v4, Coder.DUMMY_CODER);
    dagBuilder.connectVertices(e4);

    final IREdge e5 = new IREdge(DataCommunicationPatternProperty.Value.Shuffle, v2, v5, Coder.DUMMY_CODER);
    dagBuilder.connectVertices(e5);

    return dagBuilder;
  }

  private void scheduleAndCheckJobTermination(final DAG<IRVertex, IREdge> irDAG) throws InjectionException {
    final DAG<PhysicalStage, PhysicalStageEdge> physicalDAG = irDAG.convert(physicalPlanGenerator);

    final PhysicalPlan plan = new PhysicalPlan("TestPlan", physicalDAG, physicalPlanGenerator.getTaskIRVertexMap());
    final JobStateManager jobStateManager = new JobStateManager(plan, blockManagerMaster, metricMessageHandler, 1);
    scheduler.scheduleJob(plan, jobStateManager);

    // For each ScheduleGroup, test:
    // a) all stages in the ScheduleGroup enters the executing state
    // b) the stages of the next ScheduleGroup are scheduled after the stages of each ScheduleGroup are made "complete".
    for (int i = 0; i < getNumScheduleGroups(irDAG); i++) {
      final int scheduleGroupIdx = i;
      final List<PhysicalStage> stages = filterStagesWithAScheduleGroupIndex(physicalDAG, scheduleGroupIdx);

      LOG.debug("Checking that all stages of ScheduleGroup {} enter the executing state", scheduleGroupIdx);
      stages.forEach(physicalStage -> {
        while (jobStateManager.getStageState(physicalStage.getId()).getStateMachine().getCurrentState()
            != StageState.State.EXECUTING) {

        }
      });

      stages.forEach(physicalStage -> {
        RuntimeTestUtil.completeStage(
            jobStateManager, scheduler, executorRegistry, physicalStage, MAGIC_SCHEDULE_ATTEMPT_INDEX);
      });
    }

    LOG.debug("Waiting for job termination after sending stage completion events");
    while (!jobStateManager.checkJobTermination()) {
    }
    assertTrue(jobStateManager.checkJobTermination());
  }

  private List<PhysicalStage> filterStagesWithAScheduleGroupIndex(
      final DAG<PhysicalStage, PhysicalStageEdge> physicalDAG, final int scheduleGroupIndex) {
    final Set<PhysicalStage> stageSet = new HashSet<>(physicalDAG.filterVertices(
        physicalStage -> physicalStage.getScheduleGroupIndex() == scheduleGroupIndex));

    // Return the filtered vertices as a sorted list
    final List<PhysicalStage> sortedStages = new ArrayList<>(stageSet.size());
    physicalDAG.topologicalDo(stage -> {
      if (stageSet.contains(stage)) {
        sortedStages.add(stage);
      }
    });
    return sortedStages;
  }

  private int getNumScheduleGroups(final DAG<IRVertex, IREdge> irDAG) {
    final Set<Integer> scheduleGroupSet = new HashSet<>();
    irDAG.getVertices().forEach(irVertex ->
        scheduleGroupSet.add((Integer) irVertex.getProperty(ExecutionProperty.Key.ScheduleGroupIndex)));
    return scheduleGroupSet.size();
  }
}
