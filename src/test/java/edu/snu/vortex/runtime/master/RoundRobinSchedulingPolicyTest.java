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
import edu.snu.vortex.runtime.common.plan.physical.TaskGroup;
import edu.snu.vortex.runtime.executor.Executor;
import edu.snu.vortex.runtime.executor.ExecutorConfiguration;
import edu.snu.vortex.runtime.master.resourcemanager.ExecutorRepresenter;
import edu.snu.vortex.runtime.master.resourcemanager.ResourceManager;
import edu.snu.vortex.runtime.master.scheduler.RoundRobinSchedulingPolicy;
import edu.snu.vortex.runtime.master.scheduler.SchedulingPolicy;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;

/**
 * Tests {@link RoundRobinSchedulingPolicy}
 */
public final class RoundRobinSchedulingPolicyTest {
  private SchedulingPolicy schedulingPolicy;
  private ResourceManager resourceManager = new MockResourceManager();
  private final MessageSender<ControlMessage.Message> mockMsgSender = mock(MessageSender.class);
  private final ExecutorConfiguration executorConfiguration = new ExecutorConfiguration();

  private final class MockResourceManager implements ResourceManager {
    @Override
    public Optional<Executor> requestExecutor(final RuntimeAttribute resourceType,
                                final ExecutorConfiguration executorConfiguration) {
      if (resourceType == RuntimeAttribute.Compute) {
        final ExecutorRepresenter a1 = new ExecutorRepresenter("a1", RuntimeAttribute.Compute, 1, mockMsgSender);
        final ExecutorRepresenter a2 = new ExecutorRepresenter("a2", RuntimeAttribute.Compute, 1, mockMsgSender);
        final ExecutorRepresenter a3 = new ExecutorRepresenter("a3", RuntimeAttribute.Compute, 1, mockMsgSender);
        schedulingPolicy.onExecutorAdded(a3);
        schedulingPolicy.onExecutorAdded(a2);
        schedulingPolicy.onExecutorAdded(a1);
      } else {
        final ExecutorRepresenter b1 = new ExecutorRepresenter("b1", RuntimeAttribute.Storage, 1, mockMsgSender);
        final ExecutorRepresenter b2 = new ExecutorRepresenter("b2", RuntimeAttribute.Storage, 1, mockMsgSender);
        schedulingPolicy.onExecutorAdded(b2);
        schedulingPolicy.onExecutorAdded(b1);
      }
      return Optional.empty();
    }
  }

  @Before
  public void setUp() {
    schedulingPolicy = new RoundRobinSchedulingPolicy(2000);
  }

  @Test
  public void checkScheduleTimeout() {
    assertEquals(schedulingPolicy.getScheduleTimeout(), 2000);
  }

  @Test
  public void testSingleCoreTwoTypesOfExecutors() {
    resourceManager.requestExecutor(RuntimeAttribute.Compute, executorConfiguration);
    resourceManager.requestExecutor(RuntimeAttribute.Storage, executorConfiguration);

    final TaskGroup A1 = new TaskGroup("A1", "Stage A", null, RuntimeAttribute.Compute);
    final TaskGroup A2 = new TaskGroup("A2", "Stage A", null, RuntimeAttribute.Compute);
    final TaskGroup A3 = new TaskGroup("A3", "Stage A", null, RuntimeAttribute.Compute);
    final TaskGroup A4 = new TaskGroup("A4", "Stage A", null, RuntimeAttribute.Compute);
    final TaskGroup A5 = new TaskGroup("A4", "Stage A", null, RuntimeAttribute.Compute);
    final TaskGroup B1 = new TaskGroup("B1", "Stage B", null, RuntimeAttribute.Storage);
    final TaskGroup B2 = new TaskGroup("B2", "Stage B", null, RuntimeAttribute.Storage);
    final TaskGroup B3 = new TaskGroup("B3", "Stage B", null, RuntimeAttribute.Storage);

    Optional<ExecutorRepresenter> a1 = schedulingPolicy.attemptSchedule(A1);
    assertTrue(a1.isPresent());
    assertEquals(a1.get().getExecutorId(), "a1");
    schedulingPolicy.onTaskGroupScheduled(a1.get(), A1);

    Optional<ExecutorRepresenter> a2 = schedulingPolicy.attemptSchedule(A2);
    assertTrue(a2.isPresent());
    assertEquals(a2.get().getExecutorId(), "a2");
    schedulingPolicy.onTaskGroupScheduled(a2.get(), A2);

    Optional<ExecutorRepresenter> a3 = schedulingPolicy.attemptSchedule(A3);
    assertTrue(a3.isPresent());
    assertEquals(a3.get().getExecutorId(), "a3");
    schedulingPolicy.onTaskGroupScheduled(a3.get(), A3);

    Optional<ExecutorRepresenter> a4 = schedulingPolicy.attemptSchedule(A4);
    // After 2000 ms
    assertFalse(a4.isPresent());

    schedulingPolicy.onTaskGroupExecutionComplete(a1.get(), "A1");

    a4 = schedulingPolicy.attemptSchedule(A4);
    assertTrue(a4.isPresent());
    assertEquals(a4.get().getExecutorId(), "a1");
    schedulingPolicy.onTaskGroupScheduled(a1.get(), A4);

    Optional<ExecutorRepresenter> a5 = schedulingPolicy.attemptSchedule(A5);
    // After 2000 ms
    assertFalse(a5.isPresent());

    schedulingPolicy.onTaskGroupExecutionComplete(a3.get(), "A3");

    a5 = schedulingPolicy.attemptSchedule(A5);
    assertTrue(a5.isPresent());
    assertEquals(a5.get().getExecutorId(), "a3");
    schedulingPolicy.onTaskGroupScheduled(a5.get(), A5);

    Optional<ExecutorRepresenter> b1 = schedulingPolicy.attemptSchedule(B1);
    assertTrue(b1.isPresent());
    assertEquals(b1.get().getExecutorId(), "b1");
    schedulingPolicy.onTaskGroupScheduled(b1.get(), B1);

    Optional<ExecutorRepresenter> b2 = schedulingPolicy.attemptSchedule(B2);
    assertTrue(b2.isPresent());
    assertEquals(b2.get().getExecutorId(), "b2");
    schedulingPolicy.onTaskGroupScheduled(b2.get(), B2);

    Optional<ExecutorRepresenter> b3 = schedulingPolicy.attemptSchedule(B3);
    // After 2000 ms
    assertFalse(b3.isPresent());

    schedulingPolicy.onTaskGroupExecutionComplete(b1.get(), "B1");

    b3 = schedulingPolicy.attemptSchedule(B3);
    assertTrue(b3.isPresent());
    assertEquals(b3.get().getExecutorId(), "b1");
    schedulingPolicy.onTaskGroupScheduled(b3.get(), B3);

    Set<String> executingTaskGroups = schedulingPolicy.onExecutorRemoved(b1.get());
    assertEquals(executingTaskGroups.size(), 1);
    assertEquals(executingTaskGroups.iterator().next(), "B3");

    executingTaskGroups = schedulingPolicy.onExecutorRemoved(a1.get());
    assertEquals(executingTaskGroups.size(), 1);
    assertEquals(executingTaskGroups.iterator().next(), "A4");

    verify(mockMsgSender, times(8)).send(anyObject());
  }
}

