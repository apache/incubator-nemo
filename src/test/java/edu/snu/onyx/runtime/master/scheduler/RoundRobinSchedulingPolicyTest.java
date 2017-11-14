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
package edu.snu.onyx.runtime.master.scheduler;

import edu.snu.onyx.compiler.ir.executionproperty.vertex.ExecutorPlacementProperty;
import edu.snu.onyx.runtime.common.comm.ControlMessage;
import edu.snu.onyx.runtime.common.message.MessageSender;
import edu.snu.onyx.runtime.common.plan.physical.ScheduledTaskGroup;
import edu.snu.onyx.runtime.common.plan.physical.TaskGroup;
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
@PrepareForTest(ContainerManager.class)
public final class RoundRobinSchedulingPolicyTest {
  private static final int TIMEOUT_MS = 1000;

  private SchedulingPolicy schedulingPolicy;
  private ContainerManager containerManager = mock(ContainerManager.class);
  private final MessageSender<ControlMessage.Message> mockMsgSender = mock(MessageSender.class);

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

    Optional<String> a1 = schedulingPolicy.attemptSchedule(a1Wrapper);
    assertTrue(a1.isPresent());
    schedulingPolicy.onTaskGroupScheduled(a1.get(), a1Wrapper);

    Optional<String> a2 = schedulingPolicy.attemptSchedule(a2Wrapper);
    assertTrue(a2.isPresent());
    schedulingPolicy.onTaskGroupScheduled(a2.get(), a2Wrapper);

    Optional<String> a3 = schedulingPolicy.attemptSchedule(a3Wrapper);
    assertTrue(a3.isPresent());
    schedulingPolicy.onTaskGroupScheduled(a3.get(), a3Wrapper);

    Optional<String> a4 = schedulingPolicy.attemptSchedule(a4Wrapper);
    assertTrue(a4.isPresent());
    schedulingPolicy.onTaskGroupScheduled(a4.get(), a4Wrapper);

    Optional<String> a5 = schedulingPolicy.attemptSchedule(a5Wrapper);
    assertTrue(a5.isPresent());
    schedulingPolicy.onTaskGroupScheduled(a5.get(), a5Wrapper);

    // No more slot
    Optional<String> a6 = schedulingPolicy.attemptSchedule(a6Wrapper);
    assertFalse(a6.isPresent());
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

    Optional<String> a1 = schedulingPolicy.attemptSchedule(a1Wrapper);
    assertTrue(a1.isPresent());
    assertEquals("a1", a1.get());
    schedulingPolicy.onTaskGroupScheduled(a1.get(), a1Wrapper);

    Optional<String> a2 = schedulingPolicy.attemptSchedule(a2Wrapper);
    assertTrue(a2.isPresent());
    assertEquals("a2", a2.get());
    schedulingPolicy.onTaskGroupScheduled(a2.get(), a2Wrapper);

    Optional<String> a3 = schedulingPolicy.attemptSchedule(a3Wrapper);
    assertTrue(a3.isPresent());
    assertEquals("a3", a3.get());
    schedulingPolicy.onTaskGroupScheduled(a3.get(), a3Wrapper);

    Optional<String> a4 = schedulingPolicy.attemptSchedule(a4Wrapper);
    // After 2000 ms
    assertFalse(a4.isPresent());

    schedulingPolicy.onTaskGroupExecutionComplete(a1.get(), "A1");

    a4 = schedulingPolicy.attemptSchedule(a4Wrapper);
    assertTrue(a4.isPresent());
    assertEquals("a1", a4.get());
    schedulingPolicy.onTaskGroupScheduled(a1.get(), a4Wrapper);

    Optional<String> a5 = schedulingPolicy.attemptSchedule(a5Wrapper);
    // After 2000 ms
    assertFalse(a5.isPresent());

    schedulingPolicy.onTaskGroupExecutionComplete(a3.get(), "A3");

    a5 = schedulingPolicy.attemptSchedule(a5Wrapper);
    assertTrue(a5.isPresent());
    assertEquals("a3", a5.get());
    schedulingPolicy.onTaskGroupScheduled(a5.get(), a5Wrapper);

    Optional<String> b1 = schedulingPolicy.attemptSchedule(b1Wrapper);
    assertTrue(b1.isPresent());
    assertEquals("b1", b1.get());
    schedulingPolicy.onTaskGroupScheduled(b1.get(), b1Wrapper);

    Optional<String> b2 = schedulingPolicy.attemptSchedule(b2Wrapper);
    assertTrue(b2.isPresent());
    assertEquals("b2", b2.get());
    schedulingPolicy.onTaskGroupScheduled(b2.get(), b2Wrapper);

    Optional<String> b3 = schedulingPolicy.attemptSchedule(b3Wrapper);
    // After 2000 ms
    assertFalse(b3.isPresent());

    schedulingPolicy.onTaskGroupExecutionComplete(b1.get(), "B1");

    b3 = schedulingPolicy.attemptSchedule(b3Wrapper);
    assertTrue(b3.isPresent());
    assertEquals("b1", b3.get());
    schedulingPolicy.onTaskGroupScheduled(b3.get(), b3Wrapper);

    containerManager.onExecutorRemoved(b1.get());
    Set<String> executingTaskGroups = schedulingPolicy.onExecutorRemoved(b1.get());
    assertEquals(1, executingTaskGroups.size());
    assertEquals("B3", executingTaskGroups.iterator().next());

    containerManager.onExecutorRemoved(a1.get());
    executingTaskGroups = schedulingPolicy.onExecutorRemoved(a1.get());
    assertEquals(1, executingTaskGroups.size());
    assertEquals("A4", executingTaskGroups.iterator().next());

    verify(mockMsgSender, times(8)).send(anyObject());
  }

  private ScheduledTaskGroup wrap(final TaskGroup taskGroup) {
    return new ScheduledTaskGroup("TestPlan", taskGroup, Collections.emptyList(), Collections.emptyList(),
        MAGIC_SCHEDULE_ATTEMPT_INDEX);
  }
}

