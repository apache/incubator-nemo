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
import edu.snu.vortex.runtime.common.plan.physical.TaskGroup;
import edu.snu.vortex.runtime.master.scheduler.RoundRobinSchedulingPolicy;
import edu.snu.vortex.runtime.master.scheduler.SchedulingPolicy;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests {@link RoundRobinSchedulingPolicy}
 */
public final class RoundRobinSchedulingPolicyTest {
  private SchedulingPolicy schedulingPolicy;

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
    final ExecutorRepresenter a1 = new ExecutorRepresenter("a1", RuntimeAttribute.Compute, 1);
    final ExecutorRepresenter a2 = new ExecutorRepresenter("a2", RuntimeAttribute.Compute, 1);
    final ExecutorRepresenter a3 = new ExecutorRepresenter("a3", RuntimeAttribute.Compute, 1);
    final ExecutorRepresenter b1 = new ExecutorRepresenter("b1", RuntimeAttribute.Storage, 1);
    final ExecutorRepresenter b2 = new ExecutorRepresenter("b2", RuntimeAttribute.Storage, 1);

    schedulingPolicy.onExecutorAdded(a3);
    schedulingPolicy.onExecutorAdded(a2);
    schedulingPolicy.onExecutorAdded(a1);
    schedulingPolicy.onExecutorAdded(b2);
    schedulingPolicy.onExecutorAdded(b1);

    final TaskGroup A1 = new TaskGroup("A1", null, RuntimeAttribute.Compute);
    final TaskGroup A2 = new TaskGroup("A2", null, RuntimeAttribute.Compute);
    final TaskGroup A3 = new TaskGroup("A3", null, RuntimeAttribute.Compute);
    final TaskGroup A4 = new TaskGroup("A4", null, RuntimeAttribute.Compute);
    final TaskGroup A5 = new TaskGroup("A4", null, RuntimeAttribute.Compute);
    final TaskGroup B1 = new TaskGroup("B1", null, RuntimeAttribute.Storage);
    final TaskGroup B2 = new TaskGroup("B2", null, RuntimeAttribute.Storage);
    final TaskGroup B3 = new TaskGroup("B3", null, RuntimeAttribute.Storage);

    Optional<ExecutorRepresenter> scheduled = schedulingPolicy.attemptSchedule(A1);
    assertTrue(scheduled.isPresent());
    assertEquals(scheduled.get().getExecutorId(), "a1");
    schedulingPolicy.onTaskGroupScheduled(a1, "A1");

    scheduled = schedulingPolicy.attemptSchedule(A2);
    assertTrue(scheduled.isPresent());
    assertEquals(scheduled.get().getExecutorId(), "a2");
    schedulingPolicy.onTaskGroupScheduled(a2, "A2");

    scheduled = schedulingPolicy.attemptSchedule(A3);
    assertTrue(scheduled.isPresent());
    assertEquals(scheduled.get().getExecutorId(), "a3");
    schedulingPolicy.onTaskGroupScheduled(a3, "A3");

    scheduled = schedulingPolicy.attemptSchedule(A4);
    // After 2000 ms
    assertFalse(scheduled.isPresent());

    schedulingPolicy.onTaskGroupExecutionComplete(a1, "A1");

    scheduled = schedulingPolicy.attemptSchedule(A4);
    assertTrue(scheduled.isPresent());
    assertEquals(scheduled.get().getExecutorId(), "a1");
    schedulingPolicy.onTaskGroupScheduled(a1, "A4");

    scheduled = schedulingPolicy.attemptSchedule(A5);
    // After 2000 ms
    assertFalse(scheduled.isPresent());

    schedulingPolicy.onTaskGroupExecutionComplete(a3, "A3");

    scheduled = schedulingPolicy.attemptSchedule(A5);
    assertTrue(scheduled.isPresent());
    assertEquals(scheduled.get().getExecutorId(), "a3");
    schedulingPolicy.onTaskGroupScheduled(a3, "A5");

    scheduled = schedulingPolicy.attemptSchedule(B1);
    assertTrue(scheduled.isPresent());
    assertEquals(scheduled.get().getExecutorId(), "b1");
    schedulingPolicy.onTaskGroupScheduled(b1, "B1");

    scheduled = schedulingPolicy.attemptSchedule(B2);
    assertTrue(scheduled.isPresent());
    assertEquals(scheduled.get().getExecutorId(), "b2");
    schedulingPolicy.onTaskGroupScheduled(b2, "B2");

    scheduled = schedulingPolicy.attemptSchedule(B3);
    // After 2000 ms
    assertFalse(scheduled.isPresent());

    schedulingPolicy.onTaskGroupExecutionComplete(b1, "B1");

    scheduled = schedulingPolicy.attemptSchedule(B3);
    assertTrue(scheduled.isPresent());
    assertEquals(scheduled.get().getExecutorId(), "b1");
    schedulingPolicy.onTaskGroupScheduled(b1, "B3");

    Set<String> executingTaskGroups = schedulingPolicy.onExecutorRemoved(b1);
    assertEquals(executingTaskGroups.size(), 1);
    assertEquals(executingTaskGroups.iterator().next(), "B3");

    executingTaskGroups = schedulingPolicy.onExecutorRemoved(a1);
    assertEquals(executingTaskGroups.size(), 1);
    assertEquals(executingTaskGroups.iterator().next(), "A4");
  }
}
