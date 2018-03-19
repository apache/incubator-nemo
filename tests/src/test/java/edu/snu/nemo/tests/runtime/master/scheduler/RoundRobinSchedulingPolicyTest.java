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

import edu.snu.nemo.common.ir.vertex.executionproperty.ExecutorPlacementProperty;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.comm.ControlMessage;
import edu.snu.nemo.runtime.common.message.MessageSender;
import edu.snu.nemo.runtime.common.plan.physical.ScheduledTaskGroup;
import edu.snu.nemo.runtime.master.JobStateManager;
import edu.snu.nemo.runtime.master.scheduler.ExecutorRegistry;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
import edu.snu.nemo.runtime.master.resource.ResourceSpecification;
import edu.snu.nemo.runtime.master.scheduler.RoundRobinSchedulingPolicy;
import edu.snu.nemo.runtime.master.scheduler.SchedulingPolicy;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * Tests {@link RoundRobinSchedulingPolicy}
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobStateManager.class)
public final class RoundRobinSchedulingPolicyTest {
  private SchedulingPolicy schedulingPolicy;
  private ExecutorRegistry executorRegistry;
  private final MessageSender<ControlMessage.Message> mockMsgSender = mock(MessageSender.class);
  private JobStateManager jobStateManager = mock(JobStateManager.class);

  // This schedule index will make sure that task group events are not ignored
  private static final int MAGIC_SCHEDULE_ATTEMPT_INDEX = Integer.MAX_VALUE;
  private static final String RESERVED_EXECUTOR_ID = "RESERVED";

  @Before
  public void setUp() throws InjectionException {
    executorRegistry = Tang.Factory.getTang().newInjector().getInstance(ExecutorRegistry.class);

    schedulingPolicy = new RoundRobinSchedulingPolicy(executorRegistry);

    final ActiveContext activeContext = mock(ActiveContext.class);
    Mockito.doThrow(new RuntimeException()).when(activeContext).close();

    final ExecutorService serExecutorService = Executors.newSingleThreadExecutor();
    final ResourceSpecification computeSpec = new ResourceSpecification(ExecutorPlacementProperty.COMPUTE, 1, 0);
    final Function<String, ExecutorRepresenter> computeSpecExecutorRepresenterGenerator = executorId ->
        new ExecutorRepresenter(executorId, computeSpec, mockMsgSender, activeContext, serExecutorService, executorId);
    final ExecutorRepresenter a3 = computeSpecExecutorRepresenterGenerator.apply("a3");
    final ExecutorRepresenter a2 = computeSpecExecutorRepresenterGenerator.apply("a2");
    final ExecutorRepresenter a1 = computeSpecExecutorRepresenterGenerator.apply("a1");

    final ResourceSpecification storageSpec = new ResourceSpecification(ExecutorPlacementProperty.TRANSIENT, 1, 0);
    final Function<String, ExecutorRepresenter> storageSpecExecutorRepresenterGenerator = executorId ->
        new ExecutorRepresenter(executorId, storageSpec, mockMsgSender, activeContext, serExecutorService, executorId);
    final ExecutorRepresenter b2 = storageSpecExecutorRepresenterGenerator.apply("b2");
    final ExecutorRepresenter b1 = storageSpecExecutorRepresenterGenerator.apply("b1");

    final ResourceSpecification reservedSpec = new ResourceSpecification(ExecutorPlacementProperty.RESERVED, 1, 0);
    final Function<String, ExecutorRepresenter> reservedSpecExecutorRepresenterGenerator = executorId ->
        new ExecutorRepresenter(executorId, reservedSpec, mockMsgSender, activeContext, serExecutorService, executorId);
    final ExecutorRepresenter r = reservedSpecExecutorRepresenterGenerator.apply(RESERVED_EXECUTOR_ID);

    // Add compute nodes
    schedulingPolicy.onExecutorAdded(a3);
    schedulingPolicy.onExecutorAdded(a2);
    schedulingPolicy.onExecutorAdded(a1);

    // Add storage nodes
    schedulingPolicy.onExecutorAdded(b2);
    schedulingPolicy.onExecutorAdded(b1);

    // Add reserved node
    schedulingPolicy.onExecutorAdded(r);
  }

  @Test
  public void testNoneContainerType() {
    final int slots = 6;
    final List<ScheduledTaskGroup> scheduledTaskGroups =
        convertToScheduledTaskGroups(slots + 1, new byte[0], "Stage A", ExecutorPlacementProperty.NONE);

    boolean isScheduled;
    for (int i = 0; i < slots; i++) {
      isScheduled = schedulingPolicy.scheduleTaskGroup(scheduledTaskGroups.get(i), jobStateManager);
      assertTrue(isScheduled);
    }

    // No more slot
    isScheduled = schedulingPolicy.scheduleTaskGroup(scheduledTaskGroups.get(slots), jobStateManager);
    assertFalse(isScheduled);
  }

  /**
   * Wrap a DAG of a task group into {@link ScheduledTaskGroup}s.
   *
   * @param parallelism            how many scheduled task group will be generated.
   * @param serializedTaskGroupDag the serialized DAG of the task group.
   * @param stageId                the ID of the stage.
   * @param containerType          the type of container to execute the task group on.
   * @return the wrapped scheduled task groups.
   */
  private List<ScheduledTaskGroup> convertToScheduledTaskGroups(final int parallelism,
                                                                final byte[] serializedTaskGroupDag,
                                                                final String stageId,
                                                                final String containerType) {
    final List<ScheduledTaskGroup> scheduledTaskGroups = new ArrayList<>(parallelism);
    for (int taskGroupIdx = 0; taskGroupIdx < parallelism; taskGroupIdx++) {
      final String taskGroupId = RuntimeIdGenerator.generateTaskGroupId(taskGroupIdx, stageId);
      scheduledTaskGroups.add(
          new ScheduledTaskGroup("TestPlan", serializedTaskGroupDag, taskGroupId, Collections.emptyList(),
              Collections.emptyList(), MAGIC_SCHEDULE_ATTEMPT_INDEX, containerType, Collections.emptyMap()));
    }
    return scheduledTaskGroups;
  }
}

