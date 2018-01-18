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
package edu.snu.onyx.tests.runtime.master.scheduler;

import edu.snu.onyx.common.ir.vertex.executionproperty.ExecutorPlacementProperty;
import edu.snu.onyx.runtime.common.comm.ControlMessage;
import edu.snu.onyx.runtime.common.message.MessageSender;
import edu.snu.onyx.runtime.common.plan.physical.ScheduledTaskGroup;
import edu.snu.onyx.runtime.common.plan.physical.TaskGroup;
import edu.snu.onyx.runtime.master.JobStateManager;
import edu.snu.onyx.runtime.master.resource.ContainerManager;
import edu.snu.onyx.runtime.master.resource.ExecutorRepresenter;
import edu.snu.onyx.runtime.master.resource.ResourceSpecification;
import edu.snu.onyx.runtime.master.scheduler.RoundRobinSchedulingPolicy;
import edu.snu.onyx.runtime.master.scheduler.SchedulingPolicy;
import org.apache.reef.driver.context.ActiveContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;

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

    final ResourceSpecification computeSpec = new ResourceSpecification(ExecutorPlacementProperty.COMPUTE, 1, 0);
    final ExecutorRepresenter a3 = new ExecutorRepresenter("a3", computeSpec, mockMsgSender, activeContext);
    final ExecutorRepresenter a2 = new ExecutorRepresenter("a2", computeSpec, mockMsgSender, activeContext);
    final ExecutorRepresenter a1 = new ExecutorRepresenter("a1", computeSpec, mockMsgSender, activeContext);

    final ResourceSpecification storageSpec = new ResourceSpecification(ExecutorPlacementProperty.TRANSIENT, 1, 0);
    final ExecutorRepresenter b2 = new ExecutorRepresenter("b2", storageSpec, mockMsgSender, activeContext);
    final ExecutorRepresenter b1 = new ExecutorRepresenter("b1", storageSpec, mockMsgSender, activeContext);

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
    final TaskGroup A1 = new TaskGroup("A1", "Stage A", 0, null, ExecutorPlacementProperty.NONE);
    final TaskGroup A2 = new TaskGroup("A2", "Stage A", 0, null, ExecutorPlacementProperty.NONE);
    final TaskGroup A3 = new TaskGroup("A3", "Stage A", 0, null, ExecutorPlacementProperty.NONE);
    final TaskGroup A4 = new TaskGroup("A4", "Stage A", 0, null, ExecutorPlacementProperty.NONE);
    final TaskGroup A5 = new TaskGroup("A5", "Stage A", 0, null, ExecutorPlacementProperty.NONE);
    final TaskGroup A6 = new TaskGroup("A6", "Stage A", 0, null, ExecutorPlacementProperty.NONE);

    final ScheduledTaskGroup a1Wrapper = wrap(A1);
    final ScheduledTaskGroup a2Wrapper = wrap(A2);
    final ScheduledTaskGroup a3Wrapper = wrap(A3);
    final ScheduledTaskGroup a4Wrapper = wrap(A4);
    final ScheduledTaskGroup a5Wrapper = wrap(A5);
    final ScheduledTaskGroup a6Wrapper = wrap(A6);

    boolean isScheduled = schedulingPolicy.scheduleTaskGroup(a1Wrapper, jobStateManager);
    assertTrue(isScheduled);

    isScheduled= schedulingPolicy.scheduleTaskGroup(a2Wrapper, jobStateManager);
    assertTrue(isScheduled);

    isScheduled = schedulingPolicy.scheduleTaskGroup(a3Wrapper, jobStateManager);
    assertTrue(isScheduled);

    isScheduled = schedulingPolicy.scheduleTaskGroup(a4Wrapper, jobStateManager);
    assertTrue(isScheduled);

    isScheduled = schedulingPolicy.scheduleTaskGroup(a5Wrapper, jobStateManager);
    assertTrue(isScheduled);

    // No more slot
    isScheduled = schedulingPolicy.scheduleTaskGroup(a6Wrapper, jobStateManager);
    assertFalse(isScheduled);
  }

  @Test
  public void testSingleCoreTwoTypesOfExecutors() {
    final TaskGroup A1 = new TaskGroup("A1", "Stage A", 0, null, ExecutorPlacementProperty.COMPUTE);
    final TaskGroup A2 = new TaskGroup("A2", "Stage A", 1, null, ExecutorPlacementProperty.COMPUTE);
    final TaskGroup A3 = new TaskGroup("A3", "Stage A", 2, null, ExecutorPlacementProperty.COMPUTE);
    final TaskGroup A4 = new TaskGroup("A4", "Stage A", 3, null, ExecutorPlacementProperty.COMPUTE);
    final TaskGroup A5 = new TaskGroup("A4", "Stage A", 4, null, ExecutorPlacementProperty.COMPUTE);
    final TaskGroup B1 = new TaskGroup("B1", "Stage B", 0, null, ExecutorPlacementProperty.TRANSIENT);
    final TaskGroup B2 = new TaskGroup("B2", "Stage B", 1, null, ExecutorPlacementProperty.TRANSIENT);
    final TaskGroup B3 = new TaskGroup("B3", "Stage B", 2, null, ExecutorPlacementProperty.TRANSIENT);

    final ScheduledTaskGroup a1Wrapper = wrap(A1);
    final ScheduledTaskGroup a2Wrapper = wrap(A2);
    final ScheduledTaskGroup a3Wrapper = wrap(A3);
    final ScheduledTaskGroup a4Wrapper = wrap(A4);
    final ScheduledTaskGroup a5Wrapper = wrap(A5);
    final ScheduledTaskGroup b1Wrapper = wrap(B1);
    final ScheduledTaskGroup b2Wrapper = wrap(B2);
    final ScheduledTaskGroup b3Wrapper = wrap(B3);

    boolean a1 = schedulingPolicy.scheduleTaskGroup(a1Wrapper, jobStateManager);
    assertTrue(a1);

    boolean a2 = schedulingPolicy.scheduleTaskGroup(a2Wrapper, jobStateManager);
    assertTrue(a2);

    boolean a3 = schedulingPolicy.scheduleTaskGroup(a3Wrapper, jobStateManager);
    assertTrue(a3);

    boolean a4 = schedulingPolicy.scheduleTaskGroup(a4Wrapper, jobStateManager);
    // After 2000 ms
    assertFalse(a4);

    schedulingPolicy.onTaskGroupExecutionComplete("a1", "A1");

    a4 = schedulingPolicy.scheduleTaskGroup(a4Wrapper, jobStateManager);
    assertTrue(a4);

    boolean a5 = schedulingPolicy.scheduleTaskGroup(a5Wrapper, jobStateManager);
    // After 2000 ms
    assertFalse(a5);

    schedulingPolicy.onTaskGroupExecutionComplete("a3", "A3");

    a5 = schedulingPolicy.scheduleTaskGroup(a5Wrapper, jobStateManager);
    assertTrue(a5);

    boolean b1 = schedulingPolicy.scheduleTaskGroup(b1Wrapper, jobStateManager);
    assertTrue(b1);

    boolean b2 = schedulingPolicy.scheduleTaskGroup(b2Wrapper, jobStateManager);
    assertTrue(b2);

    boolean b3 = schedulingPolicy.scheduleTaskGroup(b3Wrapper, jobStateManager);
    // After 2000 ms
    assertFalse(b3);

    schedulingPolicy.onTaskGroupExecutionComplete("b1", "B1");

    b3 = schedulingPolicy.scheduleTaskGroup(b3Wrapper, jobStateManager);
    assertTrue(b3);

    containerManager.onExecutorRemoved("b1");
    Set<String> executingTaskGroups = schedulingPolicy.onExecutorRemoved("b1");
    assertEquals(1, executingTaskGroups.size());
    assertEquals("B3", executingTaskGroups.iterator().next());

    containerManager.onExecutorRemoved("a1");
    executingTaskGroups = schedulingPolicy.onExecutorRemoved("a1");
    assertEquals(1, executingTaskGroups.size());
    assertEquals("A4", executingTaskGroups.iterator().next());

    verify(mockMsgSender, times(8)).send(anyObject());
  }

  private ScheduledTaskGroup wrap(final TaskGroup taskGroup) {
    return new ScheduledTaskGroup("TestPlan", taskGroup, Collections.emptyList(), Collections.emptyList(),
        MAGIC_SCHEDULE_ATTEMPT_INDEX);
  }
}

