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
package edu.snu.vortex.runtime.master;

import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.common.message.MessageSender;
import edu.snu.vortex.runtime.common.plan.physical.ScheduledTaskGroup;
import edu.snu.vortex.runtime.common.plan.physical.TaskGroup;
import edu.snu.vortex.runtime.master.resource.ContainerManager;
import edu.snu.vortex.runtime.master.resource.ExecutorRepresenter;
import edu.snu.vortex.runtime.master.resource.ResourceSpecification;
import edu.snu.vortex.runtime.master.scheduler.RoundRobinSchedulingPolicy;
import edu.snu.vortex.runtime.master.scheduler.SchedulingPolicy;
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
  private SchedulingPolicy schedulingPolicy;
  private ContainerManager containerManager = mock(ContainerManager.class);
  private final MessageSender<ControlMessage.Message> mockMsgSender = mock(MessageSender.class);

  @Before
  public void setUp() {
    final Map<String, ExecutorRepresenter> executorRepresenterMap = new HashMap<>();
    when(containerManager.getExecutorRepresenterMap()).thenReturn(executorRepresenterMap);

    schedulingPolicy = new RoundRobinSchedulingPolicy(containerManager, 2000);

    final ActiveContext activeContext = mock(ActiveContext.class);
    Mockito.doThrow(new RuntimeException()).when(activeContext).close();

    final ResourceSpecification computeSpec = new ResourceSpecification(RuntimeAttribute.Compute, 1, 0);
    final ExecutorRepresenter a3 = new ExecutorRepresenter("a3", computeSpec, mockMsgSender, activeContext);
    final ExecutorRepresenter a2 = new ExecutorRepresenter("a2", computeSpec, mockMsgSender, activeContext);
    final ExecutorRepresenter a1 = new ExecutorRepresenter("a1", computeSpec, mockMsgSender, activeContext);

    final ResourceSpecification storageSpec = new ResourceSpecification(RuntimeAttribute.Storage, 1, 0);
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
    assertEquals(schedulingPolicy.getScheduleTimeoutMs(), 2000);
  }

  @Test
  public void testSingleCoreTwoTypesOfExecutors() {
    final TaskGroup A1 = new TaskGroup("A1", "Stage A", 0, null, RuntimeAttribute.Compute);
    final TaskGroup A2 = new TaskGroup("A2", "Stage A", 1, null, RuntimeAttribute.Compute);
    final TaskGroup A3 = new TaskGroup("A3", "Stage A", 2, null, RuntimeAttribute.Compute);
    final TaskGroup A4 = new TaskGroup("A4", "Stage A", 3, null, RuntimeAttribute.Compute);
    final TaskGroup A5 = new TaskGroup("A4", "Stage A", 4, null, RuntimeAttribute.Compute);
    final TaskGroup B1 = new TaskGroup("B1", "Stage B", 0, null, RuntimeAttribute.Storage);
    final TaskGroup B2 = new TaskGroup("B2", "Stage B", 1, null, RuntimeAttribute.Storage);
    final TaskGroup B3 = new TaskGroup("B3", "Stage B", 2, null, RuntimeAttribute.Storage);

    final ScheduledTaskGroup a1Wrapper = new ScheduledTaskGroup(A1, Collections.emptyList(), Collections.emptyList());
    final ScheduledTaskGroup a2Wrapper = new ScheduledTaskGroup(A2, Collections.emptyList(), Collections.emptyList());
    final ScheduledTaskGroup a3Wrapper = new ScheduledTaskGroup(A3, Collections.emptyList(), Collections.emptyList());
    final ScheduledTaskGroup a4Wrapper = new ScheduledTaskGroup(A4, Collections.emptyList(), Collections.emptyList());
    final ScheduledTaskGroup a5Wrapper = new ScheduledTaskGroup(A5, Collections.emptyList(), Collections.emptyList());
    final ScheduledTaskGroup b1Wrapper = new ScheduledTaskGroup(B1, Collections.emptyList(), Collections.emptyList());
    final ScheduledTaskGroup b2Wrapper = new ScheduledTaskGroup(B2, Collections.emptyList(), Collections.emptyList());
    final ScheduledTaskGroup b3Wrapper = new ScheduledTaskGroup(B3, Collections.emptyList(), Collections.emptyList());

    Optional<String> a1 = schedulingPolicy.attemptSchedule(a1Wrapper);
    assertTrue(a1.isPresent());
    assertEquals(a1.get(), "a1");
    schedulingPolicy.onTaskGroupScheduled(a1.get(), a1Wrapper);

    Optional<String> a2 = schedulingPolicy.attemptSchedule(a2Wrapper);
    assertTrue(a2.isPresent());
    assertEquals(a2.get(), "a2");
    schedulingPolicy.onTaskGroupScheduled(a2.get(), a2Wrapper);

    Optional<String> a3 = schedulingPolicy.attemptSchedule(a3Wrapper);
    assertTrue(a3.isPresent());
    assertEquals(a3.get(), "a3");
    schedulingPolicy.onTaskGroupScheduled(a3.get(), a3Wrapper);

    Optional<String> a4 = schedulingPolicy.attemptSchedule(a4Wrapper);
    // After 2000 ms
    assertFalse(a4.isPresent());

    schedulingPolicy.onTaskGroupExecutionComplete(a1.get(), "A1");

    a4 = schedulingPolicy.attemptSchedule(a4Wrapper);
    assertTrue(a4.isPresent());
    assertEquals(a4.get(), "a1");
    schedulingPolicy.onTaskGroupScheduled(a1.get(), a4Wrapper);

    Optional<String> a5 = schedulingPolicy.attemptSchedule(a5Wrapper);
    // After 2000 ms
    assertFalse(a5.isPresent());

    schedulingPolicy.onTaskGroupExecutionComplete(a3.get(), "A3");

    a5 = schedulingPolicy.attemptSchedule(a5Wrapper);
    assertTrue(a5.isPresent());
    assertEquals(a5.get(), "a3");
    schedulingPolicy.onTaskGroupScheduled(a5.get(), a5Wrapper);

    Optional<String> b1 = schedulingPolicy.attemptSchedule(b1Wrapper);
    assertTrue(b1.isPresent());
    assertEquals(b1.get(), "b1");
    schedulingPolicy.onTaskGroupScheduled(b1.get(), b1Wrapper);

    Optional<String> b2 = schedulingPolicy.attemptSchedule(b2Wrapper);
    assertTrue(b2.isPresent());
    assertEquals(b2.get(), "b2");
    schedulingPolicy.onTaskGroupScheduled(b2.get(), b2Wrapper);

    Optional<String> b3 = schedulingPolicy.attemptSchedule(b3Wrapper);
    // After 2000 ms
    assertFalse(b3.isPresent());

    schedulingPolicy.onTaskGroupExecutionComplete(b1.get(), "B1");

    b3 = schedulingPolicy.attemptSchedule(b3Wrapper);
    assertTrue(b3.isPresent());
    assertEquals(b3.get(), "b1");
    schedulingPolicy.onTaskGroupScheduled(b3.get(), b3Wrapper);

    Set<String> executingTaskGroups = schedulingPolicy.onExecutorRemoved(b1.get());
    assertEquals(executingTaskGroups.size(), 1);
    assertEquals(executingTaskGroups.iterator().next(), "B3");

    executingTaskGroups = schedulingPolicy.onExecutorRemoved(a1.get());
    assertEquals(executingTaskGroups.size(), 1);
    assertEquals(executingTaskGroups.iterator().next(), "A4");

    verify(mockMsgSender, times(8)).send(anyObject());
  }
}

