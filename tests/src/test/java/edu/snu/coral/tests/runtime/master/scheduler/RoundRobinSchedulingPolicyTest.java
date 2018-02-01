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
package edu.snu.coral.tests.runtime.master.scheduler;

import edu.snu.coral.common.ir.vertex.executionproperty.ExecutorPlacementProperty;
import edu.snu.coral.runtime.common.RuntimeIdGenerator;
import edu.snu.coral.runtime.common.comm.ControlMessage;
import edu.snu.coral.runtime.common.message.MessageSender;
import edu.snu.coral.runtime.common.plan.physical.ScheduledTaskGroup;
import edu.snu.coral.runtime.master.JobStateManager;
import edu.snu.coral.runtime.master.resource.ContainerManager;
import edu.snu.coral.runtime.master.resource.ExecutorRepresenter;
import edu.snu.coral.runtime.master.resource.ResourceSpecification;
import edu.snu.coral.runtime.master.scheduler.RoundRobinSchedulingPolicy;
import edu.snu.coral.runtime.master.scheduler.SchedulingPolicy;
import org.apache.reef.driver.context.ActiveContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;

/**
 * Tests {@link RoundRobinSchedulingPolicy}
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ContainerManager.class, JobStateManager.class})
public final class RoundRobinSchedulingPolicyTest {
  private static final int TIMEOUT_MS = 1000;

  private SchedulingPolicy schedulingPolicy;
  private ContainerManager containerManager = mock(ContainerManager.class);
  private final MessageSender<ControlMessage.Message> mockMsgSender = mock(MessageSender.class);
  private JobStateManager jobStateManager = mock(JobStateManager.class);

  // This schedule index will make sure that task group events are not ignored
  private static final int MAGIC_SCHEDULE_ATTEMPT_INDEX = Integer.MAX_VALUE;

  @Before
  public void setUp() {
    final Map<String, ExecutorRepresenter> executorRepresenterMap = new HashMap<>();
    when(containerManager.getExecutorRepresenterMap()).thenReturn(executorRepresenterMap);
    when(containerManager.getFailedExecutorRepresenterMap()).thenReturn(executorRepresenterMap);

    schedulingPolicy = new RoundRobinSchedulingPolicy(containerManager, TIMEOUT_MS);

    final ActiveContext activeContext = mock(ActiveContext.class);
    Mockito.doThrow(new RuntimeException()).when(activeContext).close();

    final ExecutorService serExecutorService = Executors.newSingleThreadExecutor();
    final ResourceSpecification computeSpec = new ResourceSpecification(ExecutorPlacementProperty.COMPUTE, 1, 0);
    final ExecutorRepresenter a3 =
        new ExecutorRepresenter("a3", computeSpec, mockMsgSender, activeContext, serExecutorService);
    final ExecutorRepresenter a2 =
        new ExecutorRepresenter("a2", computeSpec, mockMsgSender, activeContext, serExecutorService);
    final ExecutorRepresenter a1 =
        new ExecutorRepresenter("a1", computeSpec, mockMsgSender, activeContext, serExecutorService);

    final ResourceSpecification storageSpec = new ResourceSpecification(ExecutorPlacementProperty.TRANSIENT, 1, 0);
    final ExecutorRepresenter b2 =
        new ExecutorRepresenter("b2", storageSpec, mockMsgSender, activeContext, serExecutorService);
    final ExecutorRepresenter b1 =
        new ExecutorRepresenter("b1", storageSpec, mockMsgSender, activeContext, serExecutorService);

    executorRepresenterMap.put(a1.getExecutorId(), a1);
    executorRepresenterMap.put(a2.getExecutorId(), a2);
    executorRepresenterMap.put(a3.getExecutorId(), a3);
    executorRepresenterMap.put(b1.getExecutorId(), b1);
    executorRepresenterMap.put(b2.getExecutorId(), b2);

    // Add compute nodes
    schedulingPolicy.onExecutorAdded(a3.getExecutorId());
    schedulingPolicy.onExecutorAdded(a2.getExecutorId());
    schedulingPolicy.onExecutorAdded(a1.getExecutorId());

    // Add storage nodes
    schedulingPolicy.onExecutorAdded(b2.getExecutorId());
    schedulingPolicy.onExecutorAdded(b1.getExecutorId());
  }

  @Test
  public void checkScheduleTimeout() {
    assertEquals(schedulingPolicy.getScheduleTimeoutMs(), TIMEOUT_MS);
  }

  @Test
  public void testNoneContainerType() {
    final int slots = 5;
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

  @Test
  public void testSingleCoreTwoTypesOfExecutors() {
    final List<ScheduledTaskGroup> scheduledTaskGroupsA =
        convertToScheduledTaskGroups(5, new byte[0], "Stage A", ExecutorPlacementProperty.COMPUTE);
    final List<ScheduledTaskGroup> scheduledTaskGroupsB =
        convertToScheduledTaskGroups(3, new byte[0], "Stage B", ExecutorPlacementProperty.TRANSIENT);


    boolean a0 = schedulingPolicy.scheduleTaskGroup(scheduledTaskGroupsA.get(0), jobStateManager);
    assertTrue(a0);

    boolean a1 = schedulingPolicy.scheduleTaskGroup(scheduledTaskGroupsA.get(1), jobStateManager);
    assertTrue(a1);

    boolean a2 = schedulingPolicy.scheduleTaskGroup(scheduledTaskGroupsA.get(2), jobStateManager);
    assertTrue(a2);

    boolean a3 = schedulingPolicy.scheduleTaskGroup(scheduledTaskGroupsA.get(3), jobStateManager);
    // After 2000 ms
    assertFalse(a3);

    schedulingPolicy.onTaskGroupExecutionComplete("a1", scheduledTaskGroupsA.get(0).getTaskGroupId());

    a3 = schedulingPolicy.scheduleTaskGroup(scheduledTaskGroupsA.get(3), jobStateManager);
    assertTrue(a3);

    boolean a4 = schedulingPolicy.scheduleTaskGroup(scheduledTaskGroupsA.get(4), jobStateManager);
    // After 2000 ms
    assertFalse(a4);

    schedulingPolicy.onTaskGroupExecutionComplete("a3", scheduledTaskGroupsA.get(2).getTaskGroupId());

    a4 = schedulingPolicy.scheduleTaskGroup(scheduledTaskGroupsA.get(4), jobStateManager);
    assertTrue(a4);

    boolean b0 = schedulingPolicy.scheduleTaskGroup(scheduledTaskGroupsB.get(0), jobStateManager);
    assertTrue(b0);

    boolean b1 = schedulingPolicy.scheduleTaskGroup(scheduledTaskGroupsB.get(1), jobStateManager);
    assertTrue(b1);

    boolean b2 = schedulingPolicy.scheduleTaskGroup(scheduledTaskGroupsB.get(2), jobStateManager);
    // After 2000 ms
    assertFalse(b2);

    schedulingPolicy.onTaskGroupExecutionComplete("b1", scheduledTaskGroupsB.get(0).getTaskGroupId());

    b2 = schedulingPolicy.scheduleTaskGroup(scheduledTaskGroupsB.get(2), jobStateManager);
    assertTrue(b2);

    containerManager.onExecutorRemoved("b1");
    Set<String> executingTaskGroups = schedulingPolicy.onExecutorRemoved("b1");
    assertEquals(1, executingTaskGroups.size());
    assertEquals(scheduledTaskGroupsB.get(2).getTaskGroupId(), executingTaskGroups.iterator().next());

    containerManager.onExecutorRemoved("a1");
    executingTaskGroups = schedulingPolicy.onExecutorRemoved("a1");
    assertEquals(1, executingTaskGroups.size());
    assertEquals(scheduledTaskGroupsA.get(3).getTaskGroupId(), executingTaskGroups.iterator().next());

    verify(mockMsgSender, times(8)).send(anyObject());
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
      scheduledTaskGroups.add(new ScheduledTaskGroup("TestPlan", serializedTaskGroupDag, taskGroupId,
          Collections.emptyList(), Collections.emptyList(), MAGIC_SCHEDULE_ATTEMPT_INDEX, containerType));
    }
    return scheduledTaskGroups;
  }
}

